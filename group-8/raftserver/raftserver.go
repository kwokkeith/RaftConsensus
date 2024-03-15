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
	Leader    ServerState = iota // 0
	Follower                     // 1
	Candidate                    // 2
)

var (
	mutex          sync.Mutex
	serverHostPort string
	serverAddr     *net.UDPAddr
	timeOut        int
	startTime      time.Time
	state          ServerState = Follower // Start as a follower
	term           int         = 0
	lastLogIndex   int         = 0
	lastLogTerm    int         = 0
	timeOutCounter time.Timer
)

//=========================================
//=========================================
// Getter/Setter Variables
//=========================================
//=========================================

// Getter and Setter for server HostPort Address
func setServerHostPort(hostPort string) {
	mutex.Lock()
	defer mutex.Unlock()
	serverHostPort = hostPort
}

func getServerHostPort() string {
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
func startServer(serverHostPort string) {
	addr, err := net.ResolveUDPAddr("udp", serverHostPort)
	if err != nil {
		log.Fatal(err)
	}
	serverAddr = addr

	// Generate random timeout
	minTimeOut := 10
	maxTimeOut := 100
	timeOut = rand.Intn(maxTimeOut-minTimeOut+1) + minTimeOut

	// Start communication (looped to keep listening until exit)
	handleCommunication()
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

/*
startCommandInterface runs the interactive
command interface for the Server Process
*/
func startCommandInterface() {
	reader := bufio.NewReader(os.Stdin) // Create a reader for stdin
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
func handleTimeOut() {
	// Change to candidate state
	state = Candidate

	// Send out votes
	// Create an instance of RequestVoteRequest, use & to get the address of the variable
	request := &miniraft.RequestVoteRequest{
		Term:          uint64(term) + 1, // Add one to the term when becoming a candidate
		LastLogIndex:  uint64(lastLogIndex),
		LastLogTerm:   uint64(lastLogTerm),
		CandidateName: serverHostPort,
	}

	// Send vote Request to other followers
	// wrap the request in a raft message
	message := &miniraft.Raft{
		Message: &miniraft.Raft_RequestVoteRequest{
			RequestVoteRequest: request,
		},
	}

	// now call the SendMiniRaftMessage function to send the message

}

//=========================================
//=========================================
// Server Communication
//=========================================
//=========================================

/*
	This function sets up the listening port for the server to receive

messages from other servers
*/
func handleCommunication() {
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
		timeOutCounter.Stop()

		// Start a timer for time to check timeout
		timeOutCounter = *time.AfterFunc(time.Duration(timeOut), handleTimeOut)

		// Debug message to check message received
		log.Printf("From %s: %v\n", addr.String(), data[:length])

		// Handles the message based on its type
		handleMessage(data)
	}
}

/* Function to handle the message based on its type */
func handleMessage(data []byte) {

}

// Function to send the miniraft message
func SendMiniRaftMessage(conn *net.UDPConn, addr *net.UDPAddr, message *miniraft.Raft) (err error) {
	// Serialize the message
	data, err := proto.Marshal(message)
	log.Printf("SendMiniRaftMessage(): sending %s (%v), %d to %s\n", message, data, len(data), addr.String())
	if err != nil {
		log.Panicln("Failed to marshal message.", err)
	}

	// Send the message over, we dont need to send the size of the message as UDP handles it,
	// but we need to specify the address of where we are sending it to as UDP is stateless and it doesnt rmb where data should be sent
	_, err = conn.WriteToUDP(data, addr)
	if err != nil {
		log.Panicln("Failed to send message.", err)
	}
	return
}

// Function to receive the miniraft message
func ReceiveMiniRaftMessage(conn *net.UDPConn) (message *miniraft.Raft, addr *net.UDPAddr, err error) {
	// Create a buffer to read the message into
	buffer := make([]byte, 1024)

	// Read the message into the buffer
	n, addr, err := conn.ReadFromUDP(buffer)
	if err != nil {
		log.Printf("ReceiveMiniRaftMessage(): failed to read message: %v\n", err)
	}

	if n != 1024 {
		log.Printf("ReceiveMiniRaftMessage(): received %d bytes, expected 1024\n", n)
	}

	// Unmarshal the message
	message = &miniraft.Raft{}
	err = proto.Unmarshal(buffer[:n], message)
	if err != nil {
		log.Printf("ReceiveMiniRaftMessage(): failed to unmarshal message: %v\n", err)
	}
	log.Printf("ReceiveMiniRaftMessage(): received %s (%v), %d from %s\n", message, buffer[:n], n, addr.String())
	return
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
	serversFile := os.Args[2]    // Get the filename of the file containing all the servers
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
