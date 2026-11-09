package gol

import (
	"net/rpc"
	"strconv"
)

type distributorChannels struct {
	events     chan<- Event
	ioCommand  chan<- ioCommand
	ioIdle     <-chan bool
	ioFilename chan<- string
	ioOutput   chan<- uint8
	ioInput    <-chan uint8
}

// distributor acts as the RPC client, dialing the server hosted in gol.go
func distributor(p Params, c distributorChannels) {

	// 1. Create a 2D slice to store the world
	world := make([][]byte, p.ImageHeight)
	for i := range world {
		world[i] = make([]byte, p.ImageWidth)
	}

	// 2. Request initial state from IO
	c.ioCommand <- ioInput
	c.ioFilename <- strconv.Itoa(p.ImageWidth) + "x" + strconv.Itoa(p.ImageHeight)

	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			world[y][x] = <-c.ioInput
		}
	}

	turn := 0
	c.events <- StateChange{turn, Executing}

	// 3. Connect via RPC to the server hosted in gol.go
	// (Change "127.0.0.1:8030" to your AWS IP if running across machines)
	client, err := rpc.Dial("tcp", "54.224.163.245:8030")
	if err != nil {
		panic(err)
	}
	defer client.Close()

	request := Request{
		Params: p,
		Board:  world,
	}
	response := new(Response)

	// Make the single blocking RPC call
	err = client.Call(ProcessTurnsHandler, request, response)
	if err != nil {
		panic(err)
	}

	turn = p.Turns
	c.events <- TurnComplete{CompletedTurns: turn}

	// 4. Report the final state using FinalTurnComplete Event expected by tests
	c.events <- FinalTurnComplete{
		CompletedTurns: turn,
		Alive:          response.Alive,
	}

	// Make sure that the IO has finished any output before exiting.
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	c.events <- StateChange{turn, Quitting}

	// Close the channel to stop the SDL goroutine gracefully
	close(c.events)
}
