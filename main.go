package main

import (
	"encoding/hex"
	"net"
	"os"
)

const (
	HOST = "mico.lan"
	PORT = "8000"
	TYPE = "tcp"
)

func main() {
	tcpServer, err := net.ResolveTCPAddr(TYPE, HOST+":"+PORT)
	if err != nil {
		println("Resolve Failed:", err.Error())
		os.Exit(1)
	}

	conn, err := net.DialTCP(TYPE, nil, tcpServer)
	if err != nil {
		println("Dail failed:", err.Error())
		os.Exit(2)
	}

	received := make([]byte, 1024)
	for {

		numRead, err := conn.Read(received)
		if err != nil {
			println("Read data failed:", err.Error())
			os.Exit(3)
		}
		data := hex.EncodeToString(received[:numRead])
		println("%d -> %s\n", numRead, data)
	}
}
