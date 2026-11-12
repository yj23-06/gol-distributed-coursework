package main

import (
	"flag"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"

	"uk.ac.bris.cs/gameoflife/gol"
	"uk.ac.bris.cs/gameoflife/util"
)

// WorkerRequest and WorkerResponse mirror the definitions on the AWS worker nodes
type WorkerRequest struct {
	Rows          [][]byte
	TopGhost      []byte
	BottomGhost   []byte
	ImageWidth    int
	GlobalOffsetY int
}

type WorkerResponse struct {
	UpdatedRows [][]byte
	Alive       []util.Cell
}

type BrokerServer struct {
	mu           sync.Mutex
	workerAddrs  []string
	currentTurn  int
	currentAlive []util.Cell
	paused       bool
	shutdown     bool
}

// ProcessTurns receives the full board from the local controller, slices it,
// coordinates turns across AWS workers, handles broker-mediated ghost-row exchange,
// and returns the final alive cells.
func (b *BrokerServer) ProcessTurns(req gol.Request, res *gol.Response) error {
	b.mu.Lock()
	b.paused = false
	b.shutdown = false
	b.currentTurn = 0
	b.currentAlive = nil
	b.mu.Unlock()

	p := req.Params
	board := req.Board
	numWorkers := len(b.workerAddrs)
	height := p.ImageHeight
	rowsPerWorker := height / numWorkers

	for turn := 0; turn < p.Turns; turn++ {
		// Handle pause and shutdown states
		b.mu.Lock()
		for b.paused && !b.shutdown {
			b.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			b.mu.Lock()
		}
		if b.shutdown {
			b.mu.Unlock()
			return nil
		}
		b.mu.Unlock()

		var wg sync.WaitGroup
		updatedStrips := make([][][]byte, numWorkers)
		workerAliveCells := make([][]util.Cell, numWorkers)
		errs := make([]error, numWorkers)

		// Dispatch horizontal strips to each AWS worker concurrently
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerIdx int) {
				defer wg.Done()

				// Calculate row boundaries for this worker's strip
				startY := workerIdx * rowsPerWorker
				endY := (workerIdx + 1) * rowsPerWorker
				if workerIdx == numWorkers-1 {
					endY = height // Remainder rows go to the final worker
				}

				stripHeight := endY - startY
				stripRows := make([][]byte, stripHeight)
				for r := 0; r < stripHeight; r++ {
					stripRows[r] = board[startY+r]
				}

				// Determine adjacent ghost rows with toroidal wrapping
				aboveWorker := (workerIdx - 1 + numWorkers) % numWorkers
				belowWorker := (workerIdx + 1) % numWorkers

				aboveEndY := (aboveWorker + 1) * rowsPerWorker
				if aboveWorker == numWorkers-1 {
					aboveEndY = height
				}
				topGhost := board[aboveEndY-1]

				belowStartY := belowWorker * rowsPerWorker
				botGhost := board[belowStartY]

				wReq := WorkerRequest{
					Rows:          stripRows,
					TopGhost:      topGhost,
					BottomGhost:   botGhost,
					ImageWidth:    p.ImageWidth,
					GlobalOffsetY: startY,
				}

				var wRes WorkerResponse
				client, err := rpc.Dial("tcp", b.workerAddrs[workerIdx])
				if err != nil {
					errs[workerIdx] = err
					return
				}
				defer client.Close()

				err = client.Call("WorkerEngine.ComputeGeneration", wReq, &wRes)
				if err != nil {
					errs[workerIdx] = err
					return
				}

				updatedStrips[workerIdx] = wRes.UpdatedRows
				workerAliveCells[workerIdx] = wRes.Alive
			}(i)
		}
		wg.Wait()

		// Check if any worker encountered a network or execution error
		for _, err := range errs {
			if err != nil {
				return err
			}
		}

		// Reassemble the global board and compile alive cells for the next turn
		newBoard := make([][]byte, height)
		var allAlive []util.Cell
		for i := 0; i < numWorkers; i++ {
			startY := i * rowsPerWorker
			for r, row := range updatedStrips[i] {
				newBoard[startY+r] = row
			}
			allAlive = append(allAlive, workerAliveCells[i]...)
		}
		board = newBoard

		// Update broker status trackers
		b.mu.Lock()
		b.currentTurn = turn + 1
		b.currentAlive = allAlive
		b.mu.Unlock()
	}

	res.Alive = b.currentAlive
	return nil
}

func (b *BrokerServer) GetStatus(req gol.StatusRequest, res *gol.StatusResponse) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	res.Turn = b.currentTurn
	res.Alive = b.currentAlive
	return nil
}

func (b *BrokerServer) Pause(req gol.PauseRequest, res *gol.PauseResponse) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.paused = true
	return nil
}

func (b *BrokerServer) Resume(req gol.ResumeRequest, res *gol.ResumeResponse) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.paused = false
	return nil
}

func (b *BrokerServer) Shutdown(req gol.ShutdownRequest, res *gol.ShutdownResponse) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.shutdown = true
	b.paused = false
	return nil
}

func main() {
	port := flag.String("port", "8030", "Broker port")
	flag.Parse()

	// Configure your AWS EC2 instance IP addresses here
	broker := &BrokerServer{
		workerAddrs: []string{
			"52.90.223.57:8030",
			"34.230.34.138:8030",
			// Replace / add your AWS worker node IPs here
		},
	}

	if err := rpc.Register(broker); err != nil {
		log.Fatalf("Failed to register broker: %v", err)
	}

	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("Failed to start broker listener on port %s: %v", *port, err)
	}
	defer listener.Close()

	log.Printf("Game of Life Broker running on port %s, managing %d worker(s)...", *port, len(broker.workerAddrs))

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Connection accept error: %v", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}
