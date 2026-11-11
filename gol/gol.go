package gol

import (
	"net"
	"net/rpc"
	"sync"
	"time"

	"uk.ac.bris.cs/gameoflife/util"
)

var ProcessTurnsHandler = "GolEngine.ProcessTurns"

type Request struct {
	Params Params
	Board  [][]byte
}

type Response struct {
	Alive []util.Cell
}

type StatusRequest struct{}

type StatusResponse struct {
	Turn  int
	Alive []util.Cell
}

type PauseRequest struct{}
type PauseResponse struct{}

type ResumeRequest struct{}
type ResumeResponse struct{}

type ShutdownRequest struct{}
type ShutdownResponse struct{}

// GolEngine acts as the server-side RPC receiver with thread-safe control & progress tracking
type GolEngine struct {
	mu           sync.Mutex
	currentTurn  int
	currentAlive []util.Cell
	paused       bool
	shutdown     bool
}

func (g *GolEngine) ProcessTurns(req Request, res *Response) error {
	// Reset engine state at the start of every request to prevent cross-test contamination
	g.mu.Lock()
	g.paused = false
	g.shutdown = false
	g.currentTurn = 0
	g.currentAlive = nil
	g.mu.Unlock()

	world := req.Board
	p := req.Params

	for turn := 0; turn < p.Turns; turn++ {
		g.mu.Lock()
		for g.paused && !g.shutdown {
			g.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			g.mu.Lock()
		}
		if g.shutdown {
			g.mu.Unlock()
			return nil
		}
		g.mu.Unlock()

		newWorld := make([][]byte, p.ImageHeight)
		for i := range newWorld {
			newWorld[i] = make([]byte, p.ImageWidth)
		}

		for y := 0; y < p.ImageHeight; y++ {
			for x := 0; x < p.ImageWidth; x++ {
				neighbors := 0
				for dy := -1; dy <= 1; dy++ {
					for dx := -1; dx <= 1; dx++ {
						if dy == 0 && dx == 0 {
							continue
						}
						ny := (y + dy + p.ImageHeight) % p.ImageHeight
						nx := (x + dx + p.ImageWidth) % p.ImageWidth
						if world[ny][nx] == 255 {
							neighbors++
						}
					}
				}

				if world[y][x] == 255 {
					if neighbors == 2 || neighbors == 3 {
						newWorld[y][x] = 255
					} else {
						newWorld[y][x] = 0
					}
				} else {
					if neighbors == 3 {
						newWorld[y][x] = 255
					} else {
						newWorld[y][x] = 0
					}
				}
			}
		}
		world = newWorld

		var alive []util.Cell
		for y := 0; y < p.ImageHeight; y++ {
			for x := 0; x < p.ImageWidth; x++ {
				if world[y][x] == 255 {
					alive = append(alive, util.Cell{X: x, Y: y})
				}
			}
		}

		g.mu.Lock()
		g.currentTurn = turn + 1
		g.currentAlive = alive
		g.mu.Unlock()
	}

	var finalAlive []util.Cell
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			if world[y][x] == 255 {
				finalAlive = append(finalAlive, util.Cell{X: x, Y: y})
			}
		}
	}

	g.mu.Lock()
	g.currentTurn = p.Turns
	g.currentAlive = finalAlive
	g.mu.Unlock()

	res.Alive = finalAlive
	return nil
}

func (g *GolEngine) GetStatus(req StatusRequest, res *StatusResponse) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	res.Turn = g.currentTurn
	res.Alive = g.currentAlive
	return nil
}

func (g *GolEngine) Pause(req PauseRequest, res *PauseResponse) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.paused = true
	return nil
}

func (g *GolEngine) Resume(req ResumeRequest, res *ResumeResponse) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.paused = false
	return nil
}

func (g *GolEngine) Shutdown(req ShutdownRequest, res *ShutdownResponse) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.shutdown = true
	g.paused = false
	return nil
}

var serverOnce sync.Once

func startEmbeddedServer() {
	serverOnce.Do(func() {
		engine := new(GolEngine)
		_ = rpc.Register(engine)
		listener, err := net.Listen("tcp", "34.201.167.52")
		if err == nil {
			go func() {
				for {
					conn, err := listener.Accept()
					if err != nil {
						return
					}
					go rpc.ServeConn(conn)
				}
			}()
		}
	})
}

type Params struct {
	Turns       int
	Threads     int
	ImageWidth  int
	ImageHeight int
}

func Run(p Params, events chan<- Event, keyPresses <-chan rune) {
	startEmbeddedServer()

	ioCommand := make(chan ioCommand)
	ioIdle := make(chan bool)
	ioFilename := make(chan string)
	ioInput := make(chan uint8)
	ioOutput := make(chan uint8)

	ioChannels := ioChannels{
		command:  ioCommand,
		idle:     ioIdle,
		filename: ioFilename,
		output:   ioOutput,
		input:    ioInput,
	}
	go startIo(p, ioChannels)

	distributorChannels := distributorChannels{
		events:     events,
		ioCommand:  ioCommand,
		ioIdle:     ioIdle,
		ioFilename: ioFilename,
		ioOutput:   ioOutput,
		ioInput:    ioInput,
		keyPresses: keyPresses,
	}
	distributor(p, distributorChannels)
}
