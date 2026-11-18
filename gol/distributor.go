package gol

import (
	"fmt"
	"net/rpc"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"uk.ac.bris.cs/gameoflife/util"
)

// Stores an update received from the server, including whether the updates are finished or if an error occurred.
type updateResult struct {
	update TurnUpdate
	done   bool
	err    error
}

// Keeps track of how many times the distributor has been started.
var runSequence uint64

// Holds all the channels used to communicate with the I/O goroutine and handle key presses.
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
	// Create an empty world with the given image dimensions.
	world := make([][]byte, p.ImageHeight)
	for i := range world {
		world[i] = make([]byte, p.ImageWidth)
	}

	c.ioCommand <- ioInput
	c.ioFilename <- strconv.Itoa(p.ImageWidth) + "x" + strconv.Itoa(p.ImageHeight)

	// Read the pixel values and store them in the world.
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			world[y][x] = <-c.ioInput
		}
	}

	// Find all the cells that are alive in the starting world.
	turn := 0
	initialAlive := make([]util.Cell, 0)
	for y, row := range world {
		for x, value := range row {
			if value == 255 {
				initialAlive = append(initialAlive, util.Cell{X: x, Y: y})
			}
		}
	}
	if len(initialAlive) > 0 {
		c.events <- CellsFlipped{CompletedTurns: turn, Cells: initialAlive}
	}

	address := brokerAddress()
	if address == "" {
		// Connect to the broker, using the default address if none is configured.
		address = "127.0.0.1:8030"
	}
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Create a unique ID for this run so the server can keep track of its updates.
	runID := fmt.Sprintf("%d-%d", os.Getpid(), atomic.AddUint64(&runSequence, 1))
	if err := client.Call("GolEngine.StartUpdates", StartUpdatesRequest{RunID: runID}, &StartUpdatesResponse{}); err != nil {
		panic(err)
	}
	// Tell the event listener that the simulation has started.
	c.events <- StateChange{turn, Executing}
	// This channel is used to stop the goroutine that listens for server updates.
	stopUpdates := make(chan struct{})
	defer func() {
		close(stopUpdates)
		_ = client.Call("GolEngine.StopUpdates", StopUpdatesRequest{RunID: runID}, &StopUpdatesResponse{})
	}()

	// Send the starting board and simulation parameters to the server.
	request := Request{
		Params: p,
		Board:  world,
		RunID:  runID,
	}
	response := new(Response)

	// Run the simulation on the server in a separate goroutine.
	done := make(chan error, 1)
	go func() {
		done <- client.Call(ProcessTurnsHandler, request, response)
	}()

	// Listen for updates from the server and pass them to the distributor.
	updates := make(chan updateResult)
	go func() {
		for {
			var result updateResult
			response := NextUpdateResponse{}
			// Request the next update from the server.
			if err := client.Call("GolEngine.NextUpdate", NextUpdateRequest{RunID: runID}, &response); err != nil {
				result.err = err
				select {
				case updates <- result:
				case <-stopUpdates:
				}
				return
			}
			// Store the update details before sending them to the main goroutine.
			result.update = response.Update
			result.done = response.Done
			select {
			case updates <- result:
			case <-stopUpdates:
				return
			}
			// Stop listening once the server confirms that all updates have been sent.
			if response.Done {
				return
			}
		}
	}()

	// Create a ticker to report the number of live cells every two seconds.
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	lastReportedTurn := 0
	isPaused := false
	processFinished := false
	updatesFinished := false

	// Rebuild the world from the live cells and save it as a PGM image.
	outputPGM := func(currentTurn int, aliveCells []util.Cell) string {
		currentWorld := make([][]byte, p.ImageHeight)
		for i := range currentWorld {
			currentWorld[i] = make([]byte, p.ImageWidth)
		}
		// Mark each live cell on the new board.
		for _, cell := range aliveCells {
			if cell.Y < p.ImageHeight && cell.X < p.ImageWidth {
				currentWorld[cell.Y][cell.X] = 255
			}
		}

		filename := strconv.Itoa(p.ImageWidth) + "x" + strconv.Itoa(p.ImageHeight) + "x" + strconv.Itoa(currentTurn)

		// Send the image data to the I/O goroutine.
		c.ioCommand <- ioOutput
		c.ioFilename <- filename
		for y := 0; y < p.ImageHeight; y++ {
			for x := 0; x < p.ImageWidth; x++ {
				c.ioOutput <- currentWorld[y][x]
			}
		}
		return filename
	}

	// Handle key presses, server updates and periodic live-cell counts.
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

		// Report the live-cell count only while the simulation is running.
		case <-ticker.C:
			if !isPaused {
				statusReq := StatusRequest{}
				statusResp := StatusResponse{}
				err := client.Call("GolEngine.GetStatus", statusReq, &statusResp)
				// Only send an event if the simulation has advanced since the last report.
				if err == nil && statusResp.Turn > lastReportedTurn {
					lastReportedTurn = statusResp.Turn
					c.events <- AliveCellsCount{CompletedTurns: lastReportedTurn, CellsCount: len(statusResp.Alive)}
				}
			}

		case result := <-updates:
			if result.err != nil {
				panic(result.err)
			}
			if result.done {
				updatesFinished = true
				continue
			}
			// Send an event for any cells that changed during this turn.
			update := result.update
			if len(update.Flipped) > 0 {
				c.events <- CellsFlipped{
					CompletedTurns: update.CompletedTurns,
					Cells:          update.Flipped,
				}
			}
			// Tell the event listener that this turn has finished.
			c.events <- TurnComplete{CompletedTurns: update.CompletedTurns}

		case err := <-done:
			// Check whether the server finished processing the simulation successfully.
			if err != nil {
				panic(err)
			}
			processFinished = true
		}

		if processFinished && updatesFinished {
			for isPaused {
				time.Sleep(100 * time.Millisecond)
			}
			turn = p.Turns
			c.events <- FinalTurnComplete{
				CompletedTurns: turn,
				Alive:          response.Alive,
			}

			// Save the final world and wait for the I/O operation to finish.
			outputPGM(turn, response.Alive)
			c.ioCommand <- ioCheckIdle
			<-c.ioIdle

			// Tell the event listener that the simulation is quitting.
			c.events <- StateChange{turn, Quitting}
			close(c.events)
			return
		}
	}
}
