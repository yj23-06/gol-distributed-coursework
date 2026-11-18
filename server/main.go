package main

import (
	"flag"
	"log"
	"net"
	"net/rpc"

	"uk.ac.bris.cs/gameoflife/util"
)

// WorkerRequest carries a horizontal strip and its adjacent ghost rows from the broker
type WorkerRequest struct {
	Rows          [][]byte
	TopGhost      []byte
	BottomGhost   []byte
	ImageWidth    int
	GlobalOffsetY int
}

// WorkerResponse returns the updated local rows and the alive cells found in this strip
type WorkerResponse struct {
	UpdatedRows [][]byte
	Alive       []util.Cell
	Flipped     []util.Cell
}

type WorkerEngine struct{}

// ComputeGeneration processes one turn for this worker's strip using ghost rows for boundaries
func (w *WorkerEngine) ComputeGeneration(req WorkerRequest, res *WorkerResponse) error {
	localHeight := len(req.Rows)
	width := req.ImageWidth

	// Add the ghost rows to either side of our section so that we can check neighbours across worker boundaries without needing extra requests.
	// The actual rows sit between the two ghost rows.
	expandedHeight := localHeight + 2
	expanded := make([][]byte, expandedHeight)

	expanded[0] = req.TopGhost
	for i := 0; i < localHeight; i++ {
		expanded[i+1] = req.Rows[i]
	}
	expanded[expandedHeight-1] = req.BottomGhost

	// Allocate new rows for the updated local state
	newRows := make([][]byte, localHeight)
	for i := range newRows {
		newRows[i] = make([]byte, width)
	}

	var alive []util.Cell
	var flipped []util.Cell

	// Only process the rows assigned to this worker.
	// The extra rows are just there to help with neighbour calculations.
	for y := 0; y < localHeight; y++ {
		expY := y + 1
		for x := 0; x < width; x++ {
			neighbors := 0

			// Check all 8 neighbors using the expanded grid (ghost rows handle cross-worker boundaries)
			for dy := -1; dy <= 1; dy++ {
				for dx := -1; dx <= 1; dx++ {
					if dy == 0 && dx == 0 {
						continue
					}
					ny := expY + dy
					// Handle horizontal toroidal wrapping (width)
					nx := (x + dx + width) % width

					if expanded[ny][nx] == 255 {
						neighbors++
					}
				}
			}

			// Apply the standard Game of Life rules to decide whether this cell survives, dies or becomes alive.
			isAlive := expanded[expY][x] == 255
			if isAlive {
				if neighbors == 2 || neighbors == 3 {
					newRows[y][x] = 255
					alive = append(alive, util.Cell{X: x, Y: req.GlobalOffsetY + y})
				} else {
					newRows[y][x] = 0
				}
			} else {
				if neighbors == 3 {
					newRows[y][x] = 255
					alive = append(alive, util.Cell{X: x, Y: req.GlobalOffsetY + y})
				} else {
					newRows[y][x] = 0
				}
			}

			// Keep track of cells whose state changed during this generation.
			// Add the global Y coordinate because this worker only has a local section.
			if expanded[expY][x] != newRows[y][x] {
				flipped = append(flipped, util.Cell{X: x, Y: req.GlobalOffsetY + y})
			}
		}
	}

	// Send the results back to the broker once this worker has finished.
	res.UpdatedRows = newRows
	res.Alive = alive
	res.Flipped = flipped
	return nil
}

func main() {
	port := flag.String("port", "8030", "Port to listen on")
	flag.Parse()

	engine := new(WorkerEngine)
	if err := rpc.Register(engine); err != nil {
		log.Fatalf("Failed to register worker RPC engine: %v", err)
	}

	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", *port, err)
	}
	defer listener.Close()

	log.Printf("Game of Life Distributed Worker running on port %s...", *port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Connection accept error: %v", err)
			continue
		}
		// Handle each incoming connection separately so that the worker can deal with multiple RPC connections without blocking the listener.

		go rpc.ServeConn(conn)
	}
}
