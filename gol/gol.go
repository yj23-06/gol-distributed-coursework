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

var ProcessTurnsHandler = "GolEngine.ProcessTurns"

type Request struct {
	Params Params
	Board  [][]byte
	RunID  string
}

type Response struct {
	Alive []util.Cell
}

type TurnUpdate struct {
	CompletedTurns int
	Flipped        []util.Cell
}

type StartUpdatesRequest struct {
	RunID string
}

type StartUpdatesResponse struct{}

type NextUpdateRequest struct {
	RunID string
}

type NextUpdateResponse struct {
	Update TurnUpdate
	Done   bool
}

type StopUpdatesRequest struct {
	RunID string
}

type StopUpdatesResponse struct{}

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

type updateStream struct {
	updates   chan TurnUpdate
	done      chan struct{}
	cancel    chan struct{}
	completed bool
}

type UpdateHub struct {
	mu      sync.Mutex
	streams map[string]*updateStream
}

func NewUpdateHub() *UpdateHub {
	return &UpdateHub{streams: make(map[string]*updateStream)}
}

func (h *UpdateHub) Start(runID string) error {
	if runID == "" {
		return errors.New("empty update stream run ID")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.streams[runID]; exists {
		return errors.New("update stream already exists")
	}
	h.streams[runID] = &updateStream{
		updates: make(chan TurnUpdate),
		done:    make(chan struct{}),
		cancel:  make(chan struct{}),
	}
	return nil
}

func (h *UpdateHub) Publish(runID string, update TurnUpdate) error {
	if runID == "" {
		return nil
	}
	h.mu.Lock()
	stream := h.streams[runID]
	h.mu.Unlock()
	if stream == nil {
		return errors.New("update stream not found")
	}
	select {
	case stream.updates <- update:
		return nil
	case <-stream.cancel:
		return nil
	case <-stream.done:
		return errors.New("cannot publish to completed update stream")
	}
}

func (h *UpdateHub) Next(runID string) (TurnUpdate, bool, error) {
	h.mu.Lock()
	stream := h.streams[runID]
	h.mu.Unlock()
	if stream == nil {
		return TurnUpdate{}, false, errors.New("update stream not found")
	}
	select {
	case update := <-stream.updates:
		return update, false, nil
	case <-stream.done:
		return TurnUpdate{}, true, nil
	case <-stream.cancel:
		return TurnUpdate{}, true, nil
	}
}

func (h *UpdateHub) Complete(runID string) {
	if runID == "" {
		return
	}
	h.mu.Lock()
	if stream := h.streams[runID]; stream != nil && !stream.completed {
		stream.completed = true
		close(stream.done)
	}
	h.mu.Unlock()
}

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

func (g *GolEngine) updateHub() *UpdateHub {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.updates == nil {
		g.updates = NewUpdateHub()
	}
	return g.updates
}

func (g *GolEngine) StartUpdates(req StartUpdatesRequest, res *StartUpdatesResponse) error {
	if err := g.updateHub().Start(req.RunID); err != nil {
		return err
	}
	g.mu.Lock()
	g.paused = false
	g.shutdown = false
	g.currentTurn = 0
	g.currentAlive = nil
	g.mu.Unlock()
	return nil
}

func (g *GolEngine) NextUpdate(req NextUpdateRequest, res *NextUpdateResponse) error {
	update, done, err := g.updateHub().Next(req.RunID)
	if err != nil {
		return err
	}
	res.Update = update
	res.Done = done
	return nil
}

func (g *GolEngine) StopUpdates(req StopUpdatesRequest, res *StopUpdatesResponse) error {
	g.updateHub().Stop(req.RunID)
	return nil
}

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
		if g.shutdown {
			g.mu.Unlock()
			return nil
		}
		g.mu.Unlock()

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
						ny := (y + dy + p.ImageHeight) % p.ImageHeight
						nx := (x + dx + p.ImageWidth) % p.ImageWidth
						if world[ny][nx] == 255 {
							neighbors++
						}
					}
				}

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

		if err := hub.Publish(req.RunID, TurnUpdate{
			CompletedTurns: turn + 1,
			Flipped:        flipped,
		}); err != nil {
			return err
		}
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
		listener, err := net.Listen("tcp", "127.0.0.1:8030")
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

func brokerAddress() string {
	return os.Getenv("GOL_BROKER_ADDR")
}

func Run(p Params, events chan<- Event, keyPresses <-chan rune) {
	if brokerAddress() == "" {
		startEmbeddedServer()
	}

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
