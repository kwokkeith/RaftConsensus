package main

import (
	"RAFTCONSENSUS/miniraft"
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

//=========================================
//=========================================
// Variables
//=========================================
//=========================================

// For serverstate
type ServerState int
const (
    Leader ServerState = iota // 0
    Follower // 1
    Candidate // 2
)

var (
	mutex sync.Mutex
	timerMutex sync.Mutex
	timerIsActive bool = false;
	serverHostPort string
	serverAddr *net.UDPAddr
	timeOut int
	state ServerState = Follower // Start as a follower
	serversFile string // Path to the persistent storage of the list of servers
	term int = 0
	lastLogIndex int = 0
	lastLogTerm int = 0
	timeOutCounter time.Timer; 
	servers []string
)

//=========================================
//=========================================
// Getter/Setter Variables
//=========================================
//=========================================

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

//=========================================
//=========================================
// Server startup
//=========================================
//=========================================

// Function to start server on the defined address
func startServer(serverHostPort string){
	addr, err := net.ResolveUDPAddr("udp", serverHostPort)
	if err != nil { 
		log.Fatal(err)
	}
	serverAddr = addr

	// Generate random timeout
	minTimeOut := 100000;
	maxTimeOut := 1000000000;
	timeOut = rand.Intn(maxTimeOut - minTimeOut + 1) + minTimeOut

	// Start communication (looped to keep listening until exit)
	handleCommunication()	
}

/* Checks if server is in the list of servers, else append it */
func updateServerList(serverHostPort string, new_serversFile string) {
	// Open the file in read-write mode, create it if not exists
    file, err := os.OpenFile(new_serversFile, os.O_RDWR|os.O_CREATE, 0666)
    panicCheck(err)
    defer file.Close()

	// Update the current server's knowledge of the persistent storage location
	serversFile = new_serversFile

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

//=========================================
//=========================================
// Server Timeout
//=========================================
//=========================================

/* Check timeout for client */
func handleTimeOut(){
	// Change to candidate state
	state = Candidate;

	// Send out votes
	// Create an instance of RequestVoteRequest
	request := &miniraft.RequestVoteRequest{
		Term:          uint64(term) + 1, // Add one to the term when becoming a candidate
		LastLogIndex:  uint64(lastLogIndex),
		LastLogTerm:   uint64(lastLogTerm),
		CandidateName: serverHostPort,
	}

	// wrap the request in a raft message
	message := &miniraft.Raft{
		Message: &miniraft.Raft_RequestVoteRequest{
			RequestVoteRequest: request,
		},
	}

	// Send vote Request to other followers
	msg, err := proto.Marshal(message)
	if err != nil {
		log.Fatal("Error when sending voteRequest")
	}
	updateServersKnowledge() // Update the knowledge of which servers are up
	broadcastMessage(msg)
}

//=========================================
//=========================================
// Server Communication
//=========================================
//=========================================

/* This function sets up the listening port for the server to receive
messages from other servers */
func handleCommunication(){
	listener, err := net.ListenUDP("udp", serverAddr)
	if err != nil { 
		log.Fatal(err)
	}

	// Start Communication
	for { 
		data := make([]byte, 65536)
		length, addr, err := listener.ReadFromUDP(data)
		if err != nil {
			log.Fatal(err)
		}

		// Stop any timer that was running
		timerMutex.Lock()
		if timerIsActive {
			if !timeOutCounter.Stop() {
				// Try to drain the channel if the timer has already expired
				select {
				case <-timeOutCounter.C:
				default:
				}
			}
			timerIsActive = false
		}

		// Start a timer for time to check timeout
		timerIsActive = true
		timeOutCounter = *time.AfterFunc(time.Duration(timeOut), handleTimeOut);
		timerMutex.Unlock()

		// Debug message to check message received
		log.Printf("From %s: %v\n", addr.String(), data[:length])

		// Handles the message based on its type
		handleMessage(data[:length]);
	}
}

/* Function to handle the message based on its type */
func handleMessage(data []byte){
	// Unmarshal the message
	message := &miniraft.Raft{}
	err := proto.Unmarshal(data, message)
	if err != nil {
		log.Printf("Failed to unmarshal message: %v\n", err)
	}

	// Debug message to check unmarshalled message
	log.Printf("Unmarshalled message: %v\n", data)	

	// Switch between different types of protobuf message
	mutex.Lock() 
	defer mutex.Unlock() // Release the mutex
	switch msg := message.Message.(type) {
	case *miniraft.Raft_CommandName:
		fmt.Println("Received CommandName response: ", msg.CommandName)
		handleCommandName(*msg)
	case *miniraft.Raft_AppendEntriesRequest:
		fmt.Println("Received AppendEntriesRequest: ", msg.AppendEntriesRequest)
		handleAppendEntriesRequest(*msg)
	case *miniraft.Raft_AppendEntriesResponse:
		fmt.Println("Received AppendEntriesResponse: ", msg.AppendEntriesResponse)	
		handleAppendEntriesResponse(*msg)
	case *miniraft.Raft_RequestVoteRequest:
		fmt.Println("Received AppendEntriesResponse: ", msg.RequestVoteRequest)	
		handleRequestVoteRequest(*msg)
	case *miniraft.Raft_RequestVoteResponse:
		fmt.Println("Received AppendEntriesResponse: ", msg.RequestVoteResponse)
		handleRequestVoteResponse(*msg)
	default:
		log.Printf("[handleConnection] Received an unknown type of message: %T", msg)
	}
}

/* Function to broadcast msg to all servers that are in the server list */
func broadcastMessage(data []byte){
	// Iterate over the server addresses
	for _, addr := range servers {
		if addr != serverHostPort {
			conn, err := net.Dial("udp", addr)
			if err != nil {
				log.Printf("Failed to dial UDP server %s: %v", addr, err)
				continue
			}
			defer conn.Close()
	
			// Send a message to the server
			_, err = conn.Write([]byte(data))
			if err != nil {
				log.Printf("Failed to send message to %s: %v", addr, err)
				continue
			}
	
			fmt.Printf("Message sent to %s\n", addr)
		}
	}
}

//=========================================
//=========================================
// Server Message Handling
//=========================================
//=========================================

func handleCommandName(message miniraft.Raft_CommandName) {
	
}

func handleAppendEntriesRequest(message miniraft.Raft_AppendEntriesRequest) {
	
}

func handleAppendEntriesResponse(message miniraft.Raft_AppendEntriesResponse) {

}

func handleRequestVoteRequest(message miniraft.Raft_RequestVoteRequest){

}

func handleRequestVoteResponse(message miniraft.Raft_RequestVoteResponse){

}
//=========================================
//=========================================
// Server Utilities
//=========================================
//=========================================

/*
Function update the current server's knowledge of other servers 
*/
func updateServersKnowledge(){
	// Open the file in read-only mode
	file, err := os.Open(serversFile)
	if err != nil {
		log.Fatalf("Failed to open the file: %v", err)
	}
	defer file.Close()

	// Reset servers variable
	servers = nil

	// Use bufio.Scanner to read the file line by line
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		// Add the line to the servers slice
		servers = append(servers, scanner.Text())
	}

	// Check for any errors that occurred during the scan
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error occurred during file scan: %v", err)
	}
}

//=========================================
//=========================================
// Server Main
//=========================================
//=========================================

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