package main

import (
	"flag"
	"net"
	"net/rpc"

	"uk.ac.bris.cs/gameoflife/gol"
)

func main() {
	pAddr := flag.String("port", "8030", "Port to listen on")
	flag.Parse()

	// Register the Game of Life RPC engine
	_ = rpc.Register(new(gol.GolEngine))

	listener, err := net.Listen("tcp", ":"+*pAddr)
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	// Accept incoming connections from the local controller
	rpc.Accept(listener)
}
