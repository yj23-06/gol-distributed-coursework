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

// Stores the information sent to an AWS worker to process part of the board.
type WorkerRequest struct {
	Rows          [][]byte
	TopGhost      []byte
	BottomGhost   []byte
	ImageWidth    int
	GlobalOffsetY int
}

// Contains the updated rows and cell information returned by a worker.
type WorkerResponse struct {
	UpdatedRows [][]byte
	Alive       []util.Cell
	Flipped     []util.Cell
}

// Manages the AWS workers and keeps track of the overall simulation.
type BrokerServer struct {
	mu           sync.Mutex
	workerAddrs  []string
	currentTurn  int
	currentAlive []util.Cell
	paused       bool
	shutdown     bool
	updates      *gol.UpdateHub
	updatesMu    sync.Mutex
}

// Returns the update hub, creating one if it does not already exist.
func (b *BrokerServer) updateHub() *gol.UpdateHub {
	b.updatesMu.Lock()
	defer b.updatesMu.Unlock()
	if b.updates == nil {
		b.updates = gol.NewUpdateHub()
	}
	return b.updates
}

// Starts a new update stream and resets the broker's simulation status.
func (b *BrokerServer) StartUpdates(req gol.StartUpdatesRequest, res *gol.StartUpdatesResponse) error {
	if err := b.updateHub().Start(req.RunID); err != nil {
		return err
	}
	// Reset the simulation state before starting.
	b.mu.Lock()
	b.paused = false
	b.shutdown = false
	b.currentTurn = 0
	b.currentAlive = nil
	b.mu.Unlock()
	return nil
}

// Waits for the next update and returns it to the client.
func (b *BrokerServer) NextUpdate(req gol.NextUpdateRequest, res *gol.NextUpdateResponse) error {
	update, done, err := b.updateHub().Next(req.RunID)
	if err != nil {
		return err
	}
	res.Update = update
	res.Done = done
	return nil
}

// Stops the update stream when the client no longer needs it
func (b *BrokerServer) StopUpdates(req gol.StopUpdatesRequest, res *gol.StopUpdatesResponse) error {
	b.updateHub().Stop(req.RunID)
	return nil
}

// ProcessTurns receives the full board from the local controller, slices it, coordinates turns across AWS workers, handles broker-mediated ghost-row exchange, and returns the final alive cells.
func (b *BrokerServer) ProcessTurns(req gol.Request, res *gol.Response) error {
	hub := b.updateHub()
	defer hub.Complete(req.RunID)

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
		workerFlippedCells := make([][]util.Cell, numWorkers)
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

				// Determine adjacent ghost rows with wrapping
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
				workerFlippedCells[workerIdx] = wRes.Flipped
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
		var allFlipped []util.Cell
		for i := 0; i < numWorkers; i++ {
			startY := i * rowsPerWorker
			for r, row := range updatedStrips[i] {
				newBoard[startY+r] = row
			}
			allAlive = append(allAlive, workerAliveCells[i]...)
			allFlipped = append(allFlipped, workerFlippedCells[i]...)
		}
		board = newBoard

		// Update broker status trackers
		b.mu.Lock()
		b.currentTurn = turn + 1
		b.currentAlive = allAlive
		b.mu.Unlock()

		if err := hub.Publish(req.RunID, gol.TurnUpdate{
			CompletedTurns: turn + 1,
			Flipped:        allFlipped,
		}); err != nil {
			return err
		}
	}

	res.Alive = b.currentAlive
	return nil
}

// Returns the current turn and live cells to the client.
func (b *BrokerServer) GetStatus(req gol.StatusRequest, res *gol.StatusResponse) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	res.Turn = b.currentTurn
	res.Alive = b.currentAlive
	return nil
}

// Pauses the simulation by setting the paused flag.
func (b *BrokerServer) Pause(req gol.PauseRequest, res *gol.PauseResponse) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.paused = true
	return nil
}

// Resumes the simulation by clearing the paused flag.
func (b *BrokerServer) Resume(req gol.ResumeRequest, res *gol.ResumeResponse) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.paused = false
	return nil
}

// Requests the broker to stop the simulation.
func (b *BrokerServer) Shutdown(req gol.ShutdownRequest, res *gol.ShutdownResponse) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.shutdown = true
	b.paused = false
	return nil
}

func main() {
	// Read the broker port from the command line, using 8030 by default.
	port := flag.String("port", "8030", "Broker port")
	flag.Parse()

	// Configure AWS EC2 instance IP addresses
	broker := &BrokerServer{
		workerAddrs: []string{
			"3.89.189.62:8030",
			"100.53.212.156:8030",
		},
	}

	if err := rpc.RegisterName("GolEngine", broker); err != nil {
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
