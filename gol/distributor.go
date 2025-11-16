package gol

import (
	"fmt"
	"net/rpc"
	"os"
	"sync"
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
}

// worker request is send from the controller to each AWS node
// will contain the full world and the row range
type WorkerRequest struct {
	Params Params
	World  [][]byte
	StartY int
	EndY   int
}

// worker response is what the AWS node returns
type WorkerResponse struct {
	StartY  int
	Section [][]byte
	Alive   []util.Cell
}

// for which rows of the world belong to an AWS node
type nodeSection struct {
	start int
	end   int
}

// distributor divides the work between workers and interacts with other goroutines.
func distributor(p Params, c distributorChannels, keypress <-chan rune) {

	// TODO: Create a 2D slice to store the world.

	filename := fmt.Sprintf("%dx%d", p.ImageWidth, p.ImageHeight)
	c.ioCommand <- ioInput
	c.ioFilename <- filename

	world := make([][]byte, p.ImageHeight)
	for y := 0; y < p.ImageHeight; y++ {
		world[y] = make([]byte, p.ImageWidth)
		for x := 0; x < p.ImageWidth; x++ {
			world[y][x] = <-c.ioInput
		}
	}

	// have a list of the AWS nodes addresses
	workerAddresses := []string{
		"44.211.98.86:8030",
		"3.236.245.14:8030",
	}

	numWorkers := len(workerAddresses)

	// check if any workers have been dialled
	if numWorkers == 0 {
		fmt.Println("No worker addresses dialled")
		return
	}

	// dial all the workers and store the rpc clients
	clients := make([]*rpc.Client, numWorkers)
	for i, address := range workerAddresses {
		client, err := rpc.Dial("tcp", address)
		if err != nil {
			fmt.Println("Error connecting to worker", address, ":", err)
			return
		}
		clients[i] = client
	}

	// ensure that all the rpc connections are closed when distributor exits
	defer func() {
		for _, client := range clients {
			client.Close()
		}
	}()

	// Start ticker to report alive cells every 2 seconds
	ticker := time.NewTicker(2 * time.Second)
	//Channel used to sognal the goroutine to stop
	done := make(chan bool)

	var worldMutex sync.RWMutex
	var turnMu sync.RWMutex
	turn := 0

	go func() {
		for {
			select {
			// Case runs every time the timer ticks (every 2 seconds)
			case <-ticker.C:
				worldMutex.RLock()

				totalAlive := 0

				for y := 0; y < p.ImageHeight; y++ {
					for x := 0; x < p.ImageWidth; x++ {
						if world[y][x] == 255 {
							totalAlive++
						}
					}
				}

				worldMutex.RUnlock()

				turnMu.RLock()
				currentTurn := turn
				turnMu.RUnlock()

				c.events <- AliveCellsCount{
					CompletedTurns: currentTurn,
					CellsCount:     totalAlive,
				}
			case <-done:
				return
			}
		}
	}()

	// calculates which cells are alive in the inital state before a turn has been made
	initialAlive := AliveCells(world, p.ImageWidth, p.ImageHeight)
	if len(initialAlive) > 0 {
		c.events <- CellsFlipped{
			CompletedTurns: 0,
			Cells:          initialAlive}
	}

	c.events <- StateChange{turn, Executing}

	// for each turn it needs to split up the jobs,
	// such that there is one job from eahc section for each thread
	// needs to gather the results and then put them together for the newstate of world
	// TODO: Execute all turns of the Game of Life.

	//----------------------------------------------------------------------------------------------------------//
	//----------------------------------------------------------------------------------------------------------//

	// variables for step 5
	paused := false
	quitting := false

	for {
		select {
		case key := <-keypress:
			switch key {
			case 'p':
				if !paused {
					paused = true
					turnMu.RLock()
					currentTurn := turn
					turnMu.RUnlock()
					fmt.Println("paused at turn:", currentTurn)
					c.events <- StateChange{currentTurn, Paused}

				} else {
					paused = false
					turnMu.RLock()
					currentTurn := turn
					turnMu.RUnlock()
					fmt.Println("continuing")
					c.events <- StateChange{currentTurn, Executing}
				}
			case 's':
				worldMutex.RLock()
				turnMu.RLock()
				saveImage(p, c, world, turn)
				turnMu.RUnlock()
				worldMutex.RUnlock()
			case 'q':
				quitting = true
				fmt.Println("quitting, sending signal to worker")
				for _, client := range clients {
					_ = client.Call("GOLWorker.Shutdown", struct{}{}, &struct{}{})
				}
			case 'k':
				fmt.Println("shutting down full system")
				for _, client := range clients {
					_ = client.Call("GOLWorker.Shutdown", struct{}{}, &struct{}{})
				}

				worldMutex.RLock()
				saveImage(p, c, world, turn)
				worldMutex.RUnlock()

				turnMu.RLock()
				currentTurn := turn
				turnMu.RUnlock()

				c.events <- StateChange{currentTurn, Quitting}
				close(c.events)
				os.Exit(0)
			}
			continue
		default:
		}

		turnMu.RLock()
		doneTurns := turn
		turnMu.RUnlock()

		// quit if acc finished and not paused
		if quitting || (doneTurns >= p.Turns && !paused) {
			break
		}

		if paused {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// split the rows across the AWS nodes
		nodeSections := assignSections(p.ImageHeight, numWorkers)

		// collect each node's response to build overall world
		responses := make([]WorkerResponse, numWorkers)

		// ask each AWS node to process its slice
		for i, client := range clients {
			section := nodeSections[i]
			request := WorkerRequest{
				Params: p,
				World:  world,
				StartY: section.start,
				EndY:   section.end,
			}

			var response WorkerResponse

			err := client.Call("GOLWorker.ProcessTurns", request, &response)
			if err != nil {
				fmt.Println("Error calling remote worker:", err)
				quitting = true
				continue
			}

			// store the response in correct index
			responses[i] = response
		}

		// assemble a new world
		newWorld := make([][]byte, p.ImageHeight)
		for y := 0; y < p.ImageHeight; y++ {
			newWorld[y] = make([]byte, p.ImageWidth)
		}

		for _, response := range responses {
			start := response.StartY
			for i, row := range response.Section {
				newWorld[start+i] = row
			}
		}

		///// STEP 6 CELLS FLIPPED///////////
		// At the end of each turn, put all changed coordinates into a slice,
		// and then send CellsFlipped event
		// make a slice so as to compare the old row and the new row of the world
		flippedCells := make([]util.Cell, 0)

		worldMutex.RLock()

		// go row by row, then column by column
		for y := 0; y < p.ImageHeight; y++ {
			for x := 0; x < p.ImageWidth; x++ {
				if world[y][x] != newWorld[y][x] {
					flippedCells = append(flippedCells, util.Cell{X: x, Y: y})
				}
			}
		}

		worldMutex.RUnlock()

		// if there is at least one cell thats been flipped then we need to return the
		// Cells Flipped event
		if len(flippedCells) > 0 {
			turnMu.RLock()
			currentTurn := turn + 1
			turnMu.RUnlock()
			c.events <- CellsFlipped{
				CompletedTurns: currentTurn,
				Cells:          flippedCells}
		}

		worldMutex.Lock()
		world = newWorld
		worldMutex.Unlock()

		///// STEP 6 TURN COMPLETE///////////
		// At the end of each turn we need to signal that a turn is completed
		turnMu.Lock()
		turn++
		currentTurn := turn
		turnMu.Unlock()

		c.events <- TurnComplete{
			CompletedTurns: currentTurn,
		}
	}

	//Stop ticker after finishing all turns
	done <- true
	ticker.Stop()

	// TODO: Report the final state using FinalTurnCompleteEvent.
	worldMutex.RLock()
	aliveCells := AliveCells(world, p.ImageWidth, p.ImageHeight)
	worldMutex.RUnlock()

	turnMu.RLock()
	finalTurn := turn
	turnMu.RUnlock()

	c.events <- FinalTurnComplete{CompletedTurns: finalTurn, Alive: aliveCells}

	//save final output
	worldMutex.RLock()
	saveImage(p, c, world, turn)
	worldMutex.RUnlock()

	c.events <- StateChange{finalTurn, Quitting}

	//Close the channels to stop the SDL goroutine gracefully. Removing may cause deadlock.
	close(c.events)

}

func AliveCells(world [][]byte, width, height int) []util.Cell {
	cells := make([]util.Cell, 0)
	for dy := 0; dy < height; dy++ {
		for dx := 0; dx < width; dx++ {
			if world[dy][dx] == 255 {
				cells = append(cells, util.Cell{X: dx, Y: dy})
			}
		}
	}
	return cells
}

// helper function to handle image saves
func saveImage(p Params, c distributorChannels, world [][]byte, turn int) {
	// Write final world to output file (PGM)
	// Construct the output filename in the required format
	// Example: "512x512x100" for a 512x512 world after 100 turns
	outFileName := fmt.Sprintf("%dx%dx%d", p.ImageWidth, p.ImageHeight, turn)
	c.ioCommand <- ioOutput     // telling the i/o goroutine that we are starting an output operation
	c.ioFilename <- outFileName // sending the filename to io goroutine

	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			//writing the pixel value to the ioOutput channel
			c.ioOutput <- world[y][x] //grayscale value for that pixel (0 or 255)
		}
	}

	// Make sure that the Io has finished any output before exiting.
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	// once saved, notify the SDL event system (important for Step 5)
	c.events <- ImageOutputComplete{CompletedTurns: turn, Filename: outFileName}

}

// helper function to split the rows of the image between the AWS nodes
func assignSections(height, numNodes int) []nodeSection {

	// we need to calculate the minimum number of rows for each worker
	minRows := height / numNodes
	// then say if we have extra rows left over then we need to assign those evenly to each worker
	extraRows := height % numNodes

	// make a slice, the size of the number of threads
	sections := make([]nodeSection, numNodes)
	start := 0

	for i := 0; i < numNodes; i++ {
		// assigns the base amount of rows to the thread
		rows := minRows
		// if say we're on worker 2 and there are 3 extra rows left,
		// then we can add 1 more job to the thread
		if i < extraRows {
			rows++
		}

		// marks where the end of the section ends
		end := start + rows
		// assigns these rows to the section
		sections[i] = nodeSection{start: start, end: end}
		// start is updated for the next worker
		start = end
	}
	return sections
}
