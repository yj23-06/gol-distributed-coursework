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
	Rows          [][]byte // The local horizontal rows assigned to this worker
	TopGhost      []byte   // The bottom row of the worker above (or wrapped)
	BottomGhost   []byte   // The top row of the worker below (or wrapped)
	ImageWidth    int      // Total width of the global board
	GlobalOffsetY int      // The starting Y index of this strip in the global grid
}

// WorkerResponse returns the updated local rows and the alive cells found in this strip
type WorkerResponse struct {
	UpdatedRows [][]byte
	Alive       []util.Cell
}

type WorkerEngine struct{}

// ComputeGeneration processes one turn for this worker's strip using ghost rows for boundaries
func (w *WorkerEngine) ComputeGeneration(req WorkerRequest, res *WorkerResponse) error {
	localHeight := len(req.Rows)
	width := req.ImageWidth

	// Construct an expanded working grid that includes the top and bottom ghost rows
	// Expanded grid layout:
	// Index 0: TopGhost
	// Indices 1 to localHeight: req.Rows
	// Index localHeight + 1: BottomGhost
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

	// Compute Game of Life rules for each cell in the local strip
	// Note: y in req.Rows corresponds to expanded index y + 1
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

			// Standard Game of Life rules
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
		}
	}

	res.UpdatedRows = newRows
	res.Alive = alive
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
		go rpc.ServeConn(conn)
	}
}
