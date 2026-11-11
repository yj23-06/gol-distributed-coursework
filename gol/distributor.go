package gol

import (
	"fmt"
	"net/rpc"
	"strconv"
	"time"

	"uk.ac.bris.cs/gameoflife/util"
)

type distributorChannels struct {
	events     chan<- Event
	ioCommand  chan<- ioCommand
	ioIdle     <-chan bool
	ioFilename chan<- string
	ioOutput   chan<- uint8
	ioInput    <-chan uint8
	keyPresses <-chan rune
}

func distributor(p Params, c distributorChannels) {
	world := make([][]byte, p.ImageHeight)
	for i := range world {
		world[i] = make([]byte, p.ImageWidth)
	}

	c.ioCommand <- ioInput
	c.ioFilename <- strconv.Itoa(p.ImageWidth) + "x" + strconv.Itoa(p.ImageHeight)

	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			world[y][x] = <-c.ioInput
		}
	}

	turn := 0
	c.events <- StateChange{turn, Executing}

	client, err := rpc.Dial("tcp", "127.0.0.1:8030")
	if err != nil {
		panic(err)
	}
	defer client.Close()

	request := Request{
		Params: p,
		Board:  world,
	}
	response := new(Response)

	done := make(chan error, 1)
	go func() {
		done <- client.Call(ProcessTurnsHandler, request, response)
	}()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	lastReportedTurn := 0
	isPaused := false

	outputPGM := func(currentTurn int, aliveCells []util.Cell) string {
		currentWorld := make([][]byte, p.ImageHeight)
		for i := range currentWorld {
			currentWorld[i] = make([]byte, p.ImageWidth)
		}
		for _, cell := range aliveCells {
			if cell.Y < p.ImageHeight && cell.X < p.ImageWidth {
				currentWorld[cell.Y][cell.X] = 255
			}
		}

		filename := strconv.Itoa(p.ImageWidth) + "x" + strconv.Itoa(p.ImageHeight) + "x" + strconv.Itoa(currentTurn)

		c.ioCommand <- ioOutput
		c.ioFilename <- filename
		for y := 0; y < p.ImageHeight; y++ {
			for x := 0; x < p.ImageWidth; x++ {
				c.ioOutput <- currentWorld[y][x]
			}
		}
		return filename
	}

	for {
		select {
		case key := <-c.keyPresses:
			statusResp := StatusResponse{}
			_ = client.Call("GolEngine.GetStatus", StatusRequest{}, &statusResp)

			switch key {
			case 's', 'S':
				filename := outputPGM(statusResp.Turn, statusResp.Alive)
				c.ioCommand <- ioCheckIdle
				<-c.ioIdle
				c.events <- ImageOutputComplete{CompletedTurns: statusResp.Turn, Filename: filename}

			case 'q', 'Q':
				filename := outputPGM(statusResp.Turn, statusResp.Alive)
				c.ioCommand <- ioCheckIdle
				<-c.ioIdle
				c.events <- ImageOutputComplete{CompletedTurns: statusResp.Turn, Filename: filename}

				c.events <- StateChange{statusResp.Turn, Quitting}
				close(c.events)
				return

			case 'k', 'K':
				_ = client.Call("GolEngine.Shutdown", ShutdownRequest{}, &ShutdownResponse{})
				filename := outputPGM(statusResp.Turn, statusResp.Alive)
				c.ioCommand <- ioCheckIdle
				<-c.ioIdle
				c.events <- ImageOutputComplete{CompletedTurns: statusResp.Turn, Filename: filename}

				c.events <- StateChange{statusResp.Turn, Quitting}
				close(c.events)
				return

			case 'p', 'P':
				if !isPaused {
					isPaused = true
					_ = client.Call("GolEngine.Pause", PauseRequest{}, &PauseResponse{})
					fmt.Printf("Paused at turn %d\n", statusResp.Turn)
					c.events <- StateChange{statusResp.Turn, Paused}
				} else {
					isPaused = false
					_ = client.Call("GolEngine.Resume", ResumeRequest{}, &ResumeResponse{})
					// Fetch fresh status turn after resuming
					_ = client.Call("GolEngine.GetStatus", StatusRequest{}, &statusResp)
					fmt.Println("Continuing")
					c.events <- StateChange{statusResp.Turn, Executing}
				}
			}

		case <-ticker.C:
			if !isPaused {
				statusReq := StatusRequest{}
				statusResp := StatusResponse{}
				err := client.Call("GolEngine.GetStatus", statusReq, &statusResp)
				if err == nil && statusResp.Turn > lastReportedTurn {
					lastReportedTurn = statusResp.Turn
					c.events <- AliveCellsCount{CompletedTurns: lastReportedTurn, CellsCount: len(statusResp.Alive)}
				}
			}

		case err := <-done:
			if err != nil {
				panic(err)
			}

			for isPaused {
				time.Sleep(100 * time.Millisecond)
			}

			turn = p.Turns
			for t := 1; t <= p.Turns; t++ {
				c.events <- TurnComplete{CompletedTurns: t}
			}

			c.events <- FinalTurnComplete{
				CompletedTurns: turn,
				Alive:          response.Alive,
			}

			outputPGM(turn, response.Alive)
			c.ioCommand <- ioCheckIdle
			<-c.ioIdle

			c.events <- StateChange{turn, Quitting}
			close(c.events)
			return
		}
	}
}
