package main

import ( 
	"log"
	"net"
)

func startServer(){
	addr, err := net.ResolveUDPAddr("udp", ":2000")
	if err != nil { 
		log.Fatal(err)
	}
	listener, err := net.ListenUDP("udp", addr)
	if err != nil { 
		log.Fatal(err)
	}
	for { 
		data := make([]byte, 65536)
		length, addr, err := listener.ReadFromUDP(data)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("From %s: %v\n", addr.String(), data[:length])
	}
}