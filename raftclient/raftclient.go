package main 

import ( 
	"log"
	"net"
)

func startClient() { 
	conn, err := net.ListenPacket("udp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	dst, err := net.ResolveUDPAddr("udp", "localhost:2000")
	if err != nil {
		log.Fatal(err)
	}
	_, err = conn.WriteTo([]byte("data"), dst)
	if err != nil {
		log.Fatal(err)
	}
}