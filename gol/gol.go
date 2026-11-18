package gol

import (
	"errors"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"uk.ac.bris.cs/gameoflife/util"
)

// Stores the name of the RPC method used to run the simulation.
var ProcessTurnsHandler = "GolEngine.ProcessTurns"

// Contains the information sent from the distributor to the server.
type Request struct {
	Params Params
	Board  [][]byte
	RunID  string
}

// Holds the final list of live cells returned by the server.
type Response struct {
	Alive []util.Cell
}

// Stores the changes made during one turn of the simulation.
type TurnUpdate struct {
	CompletedTurns int
	Flipped        []util.Cell
}

// Used to request a new update stream for a simulation.
type StartUpdatesRequest struct {
	RunID string
}

type StartUpdatesResponse struct{}

// Used to request the next available update from the server.
type NextUpdateRequest struct {
	RunID string
}

type NextUpdateResponse struct {
	Update TurnUpdate
	Done   bool
}

// Used to stop an update stream when it is no longer needed
type StopUpdatesRequest struct {
	RunID string
}

type StopUpdatesResponse struct{}

// Used to request the current simulation status
type StatusRequest struct{}

// Returns the current turn and the cells that are alive
type StatusResponse struct {
	Turn  int
	Alive []util.Cell
}

// Requests for pausing and resuming the simulation.
type PauseRequest struct{}
type PauseResponse struct{}
type ResumeRequest struct{}
type ResumeResponse struct{}

// Requests the server to shut down the simulation.
type ShutdownRequest struct{}
type ShutdownResponse struct{}

// Holds the channels and status information for one update stream.
type updateStream struct {
	updates   chan TurnUpdate
	done      chan struct{}
	cancel    chan struct{}
	completed bool
}

// Manages all the update streams for different simulation runs.
type UpdateHub struct {
	mu      sync.Mutex
	streams map[string]*updateStream
}

// Creates a new update hub with an empty map of streams.
func NewUpdateHub() *UpdateHub {
	return &UpdateHub{streams: make(map[string]*updateStream)}
}

// Starts a new update stream for the given simulation.
func (h *UpdateHub) Start(runID string) error {
	if runID == "" { // Check that the run ID is valid.
		return errors.New("empty update stream run ID")
	}
	h.mu.Lock() // Lock the hub while checking and updating the streams map.
	defer h.mu.Unlock()
	// Prevent two streams from using the same run ID.
	if _, exists := h.streams[runID]; exists {
		return errors.New("update stream already exists")
	}
	// Create the channels used to send updates and signal completion or cancellation.
	h.streams[runID] = &updateStream{
		updates: make(chan TurnUpdate),
		done:    make(chan struct{}),
		cancel:  make(chan struct{}),
	}
	return nil
}

// Sends a completed turn update to the correct update stream.
func (h *UpdateHub) Publish(runID string, update TurnUpdate) error {
	if runID == "" { // Ignore updates that do not belong to a specific run.
		return nil
	}
	// Find the stream associated with this run ID.
	h.mu.Lock()
	stream := h.streams[runID]
	h.mu.Unlock()
	if stream == nil {
		return errors.New("update stream not found")
	}
	// Send the update unless the stream has been cancelled or completed.
	select {
	case stream.updates <- update:
		return nil
	case <-stream.cancel:
		return nil
	case <-stream.done:
		return errors.New("cannot publish to completed update stream")
	}
}

// Waits for the next update from a particular simulation.
func (h *UpdateHub) Next(runID string) (TurnUpdate, bool, error) {
	// Find the stream using its run ID.
	h.mu.Lock()
	stream := h.streams[runID]
	h.mu.Unlock()
	if stream == nil {
		return TurnUpdate{}, false, errors.New("update stream not found")
	}
	// Wait for an update, completion signal or cancellation
	select {
	case update := <-stream.updates:
		return update, false, nil
	case <-stream.done:
		return TurnUpdate{}, true, nil
	case <-stream.cancel:
		return TurnUpdate{}, true, nil
	}
}

// Marks an update stream as completed once the simulation finishes.
func (h *UpdateHub) Complete(runID string) {
	if runID == "" {
		return
	}
	h.mu.Lock()
	// Close the done channel only if the stream exists and is not already completed.
	if stream := h.streams[runID]; stream != nil && !stream.completed {
		stream.completed = true
		close(stream.done)
	}
	h.mu.Unlock()
}

// Removes an update stream and signals that it should stop.
func (h *UpdateHub) Stop(runID string) {
	h.mu.Lock()
	if stream := h.streams[runID]; stream != nil {
		delete(h.streams, runID)
		close(stream.cancel)
	}
	h.mu.Unlock()
}

// GolEngine acts as the server-side RPC receiver with thread-safe control & progress tracking
type GolEngine struct {
	mu           sync.Mutex
	currentTurn  int
	currentAlive []util.Cell
	paused       bool
	shutdown     bool
	updates      *UpdateHub
}

// Returns the update hub, creating one if it does not already exist.
func (g *GolEngine) updateHub() *UpdateHub {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.updates == nil {
		g.updates = NewUpdateHub()
	}
	return g.updates
}

// Starts tracking updates and resets the simulation status.
func (g *GolEngine) StartUpdates(req StartUpdatesRequest, res *StartUpdatesResponse) error {
	// Create a new update stream for this run.
	if err := g.updateHub().Start(req.RunID); err != nil {
		return err
	}

	// Reset the server state before starting the simulation.
	g.mu.Lock()
	g.paused = false
	g.shutdown = false
	g.currentTurn = 0
	g.currentAlive = nil
	g.mu.Unlock()
	return nil
}

// Waits for and returns the next update from the requested stream.
func (g *GolEngine) NextUpdate(req NextUpdateRequest, res *NextUpdateResponse) error {
	update, done, err := g.updateHub().Next(req.RunID)
	if err != nil {
		return err
	}
	res.Update = update
	res.Done = done
	return nil
}

// Stops the update stream for a simulation.
func (g *GolEngine) StopUpdates(req StopUpdatesRequest, res *StopUpdatesResponse) error {
	g.updateHub().Stop(req.RunID)
	return nil
}

// Runs the Game of Life simulation on the server.
func (g *GolEngine) ProcessTurns(req Request, res *Response) error {
	hub := g.updateHub()
	defer hub.Complete(req.RunID)

	world := req.Board
	p := req.Params

	for turn := 0; turn < p.Turns; turn++ {
		g.mu.Lock()
		for g.paused && !g.shutdown {
			g.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			g.mu.Lock()
		}
		// Stop the simulation if a shutdown has been requested.
		if g.shutdown {
			g.mu.Unlock()
			return nil
		}
		g.mu.Unlock()

		// Create a new empty board to store the next generation.
		newWorld := make([][]byte, p.ImageHeight)
		for i := range newWorld {
			newWorld[i] = make([]byte, p.ImageWidth)
		}

		var flipped []util.Cell
		for y := 0; y < p.ImageHeight; y++ {
			for x := 0; x < p.ImageWidth; x++ {
				neighbors := 0
				for dy := -1; dy <= 1; dy++ {
					for dx := -1; dx <= 1; dx++ {
						if dy == 0 && dx == 0 {
							continue
						}
						// Wrap around the edges so the board behaves like a donut
						ny := (y + dy + p.ImageHeight) % p.ImageHeight
						nx := (x + dx + p.ImageWidth) % p.ImageWidth
						if world[ny][nx] == 255 {
							neighbors++
						}
					}
				}

				// Apply the Game of Life rules to decide the cell's next state.
				next := byte(0)
				if world[y][x] == 255 {
					if neighbors == 2 || neighbors == 3 {
						next = 255
					}
				} else if neighbors == 3 {
					next = 255
				}
				newWorld[y][x] = next
				if world[y][x] != next {
					flipped = append(flipped, util.Cell{X: x, Y: y})
				}
			}
		}
		world = newWorld

		var alive []util.Cell
		// Collect the positions of all cells that are still alive.
		for y := 0; y < p.ImageHeight; y++ {
			for x := 0; x < p.ImageWidth; x++ {
				if world[y][x] == 255 {
					alive = append(alive, util.Cell{X: x, Y: y})
				}
			}
		}

		// Update the shared simulation status safely using the mutex.
		g.mu.Lock()
		g.currentTurn = turn + 1
		g.currentAlive = alive
		g.mu.Unlock()

		if err := hub.Publish(req.RunID, TurnUpdate{
			CompletedTurns: turn + 1,
			Flipped:        flipped,
		}); err != nil {
			return err
		}
	}

	// Collect the live cells from the final board.
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

// Returns the current turn number and live cells to the client.
func (g *GolEngine) GetStatus(req StatusRequest, res *StatusResponse) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	res.Turn = g.currentTurn
	res.Alive = g.currentAlive
	return nil
}

// Pauses the simulation by updating the shared paused flag
func (g *GolEngine) Pause(req PauseRequest, res *PauseResponse) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.paused = true
	return nil
}

// Resumes the simulation by clearing the paused flag.
func (g *GolEngine) Resume(req ResumeRequest, res *ResumeResponse) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.paused = false
	return nil
}

// Requests the simulation to stop running.
func (g *GolEngine) Shutdown(req ShutdownRequest, res *ShutdownResponse) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.shutdown = true
	g.paused = false
	return nil
}

// Ensures that the embedded server is only started once.
var serverOnce sync.Once

func startEmbeddedServer() {
	serverOnce.Do(func() {
		engine := new(GolEngine)
		_ = rpc.Register(engine)
		listener, err := net.Listen("tcp", "127.0.0.1:8030")
		if err == nil {
			go func() {
				// Accept incoming connections and handle each one separately.
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

// Stores the settings for a Game of Life simulation.
type Params struct {
	Turns       int
	Threads     int
	ImageWidth  int
	ImageHeight int
}

// Returns the broker address from the environment variable.
func brokerAddress() string {
	return os.Getenv("GOL_BROKER_ADDR")
}

// Sets up the I/O channels and starts the distributor.
func Run(p Params, events chan<- Event, keyPresses <-chan rune) {
	if brokerAddress() == "" {
		startEmbeddedServer()
	}

	// Create the channels used for communication with the I/O goroutine.
	ioCommand := make(chan ioCommand)
	ioIdle := make(chan bool)
	ioFilename := make(chan string)
	ioInput := make(chan uint8)
	ioOutput := make(chan uint8)

	// Group the I/O channels together for the I/O handler
	ioChannels := ioChannels{
		command:  ioCommand,
		idle:     ioIdle,
		filename: ioFilename,
		output:   ioOutput,
		input:    ioInput,
	}
	go startIo(p, ioChannels)

	// Group the channels needed by the distributor.
	distributorChannels := distributorChannels{
		events:     events,
		ioCommand:  ioCommand,
		ioIdle:     ioIdle,
		ioFilename: ioFilename,
		ioOutput:   ioOutput,
		ioInput:    ioInput,
		keyPresses: keyPresses,
	}
	// Start the distributor, which coordinates the simulation and handles events.
	distributor(p, distributorChannels)
}
