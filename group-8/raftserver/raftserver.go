package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
)

var (
	mutex sync.Mutex
	serverHostPort string
	serverAddr *net.UDPAddr
)

// Getter and Setter for server HostPort Address
func setServerHostPort(hostPort string){
	mutex.Lock()
	defer mutex.Unlock()
	serverHostPort = hostPort 
}

func getServerHostPort() string{
	mutex.Lock()
	defer mutex.Unlock()
	return serverHostPort
}

// Function to start server on the defined address
func startServer(serverHostPort string){
	addr, err := net.ResolveUDPAddr("udp", serverHostPort)
	if err != nil { 
		log.Fatal(err)
	}
	serverAddr = addr

	listener, err := net.ListenUDP("udp", serverAddr)
	if err != nil { 
		log.Fatal(err)
	}

	for { 
		data := make([]byte, 65536)
		length, addr, err := listener.ReadFromUDP(data)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("From %s: %v\n", addr.String(), string(data[:length]))
	}
}

/* Checks if server is in the list of servers, else append it */
func updateServerList(serverHostPort string, serversFile string) {
	// Open the file in read-write mode, create it if not exists
    file, err := os.OpenFile(serversFile, os.O_RDWR|os.O_CREATE, 0666)
    panicCheck(err)
    defer file.Close()

    // Check if the current address is already in the file
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        if scanner.Text() == serverHostPort {
			// defined serverHostPort already exist in the list
			// Throw error and leave the program
			log.Printf("Server address %s defined already exist in %s", serverHostPort, serversFile)
			os.Exit(1)
        }
    }

    err = scanner.Err()
	panicCheck(err)

    // Append the current address if it doesn't exist in the file
	_, err = file.WriteString(serverHostPort + "\n")
	panicCheck(err)
	
	// Debug: Status announce that address has been added to the file containing list of servers
	fmt.Printf("%s added into %s\n", serverHostPort, serversFile)
}

// Checks if there is error, if so, then panic
func panicCheck(e error) {
    if e != nil {
        panic(e)
    }
}

/* startCommandInterface runs the interactive 
 command interface for the Server Process */
func startCommandInterface() {
	reader := bufio.NewReader(os.Stdin)                                      // Create a reader for stdin
	fmt.Println("Server Process command interface started. Enter commands:")

	for {
		fmt.Print("> ")                     // Print the command prompt
		cmd, err := reader.ReadString('\n') // Read a command from stdin
		if err != nil {
			fmt.Println("Error reading command:", err) // Handle errors in reading commands
			continue
		}
		cmd = strings.TrimSpace(cmd) // Remove trailing newline characters from the command

		// Switch between different command keywords
		switch cmd {
			case "exit":
				// Handle the 'exit' command.
				fmt.Println("Exiting Server Process.") 
				log.Printf("[startCommandInterface] Exit command received. serverHostPort: %s", serverHostPort)
				os.Exit(0)
			default:
				// TODO: implement different commands of server
				return
		}
	}
}

// main is the entry point of the program
func main() {
	if len(os.Args) != 3 {
		fmt.Println("Server Usage: go run raftserver.go <server-host:server-port> filename_of_serverList")
		return
	}

	serverHostPort := os.Args[1] // Get the server address from command-line arguments
	serversFile := os.Args[2] // Get the filename of the file containing all the servers
	if !strings.Contains(serverHostPort, ":") {
		fmt.Println("Invalid server address. Must be in the format host:port.") // Validate the registry address format
		return
	}

	// Debugging statement to check the initial value of serverHostPort
	log.Printf("[main] raftServer connecting to serverHostPort: %s", serverHostPort)

	// Sets the server's HostPort value for this client
	setServerHostPort(os.Args[1])

	// Check if server exist in the server list and if not append it to the list
	// If server exist then throw an error and do not start new server with defined address
	updateServerList(serverHostPort, serversFile)

	// Starts the server on the defined address
	go startServer(getServerHostPort())

	// Start the interactive command interface
	startCommandInterface() 
}