package gol

import (
	"errors"
	"net"
	"net/rpc"
	"sync"

	"uk.ac.bris.cs/gameoflife/util"
)

// ProcessTurnsHandler defines the RPC method path
var ProcessTurnsHandler = "GolEngine.ProcessTurns"

type Request struct {
	Params Params
	Board  [][]byte
}

type Response struct {
	Alive []util.Cell
}

// GolEngine acts as the server-side RPC receiver
type GolEngine struct{}

func (g *GolEngine) ProcessTurns(req Request, res *Response) error {
	if req.Params.Turns <= 0 {
		return errors.New("a number of turns must be specified")
	}

	world := req.Board
	p := req.Params

	// Run Game of Life generations on the server side
	for turn := 0; turn < p.Turns; turn++ {
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
	}

	// Gather final alive cells
	var finalAlive []util.Cell
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			if world[y][x] == 255 {
				finalAlive = append(finalAlive, util.Cell{X: x, Y: y})
			}
		}
	}

	res.Alive = finalAlive
	return nil
}

// Ensures the embedded RPC server listener starts only once
var serverOnce sync.Once

func startEmbeddedServer() {
	serverOnce.Do(func() {
		engine := new(GolEngine)
		_ = rpc.Register(engine)
		listener, err := net.Listen("tcp", ":8030")
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

// Params provides the details of how to run the Game of Life and which image to load.
type Params struct {
	Turns       int
	Threads     int
	ImageWidth  int
	ImageHeight int
}

// Run starts the processing of Game of Life. It initialises channels and goroutines.
func Run(p Params, events chan<- Event, keyPresses <-chan rune) {
	// Spin up the server listener automatically when Run is called
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
	}
	distributor(p, distributorChannels)
}
