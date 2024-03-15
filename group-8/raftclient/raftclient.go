package main

import (
	"RAFTCONSENSUS/miniraft"
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"unicode"

	"google.golang.org/protobuf/proto"
)

//=========================================
//=========================================
// Variables
//=========================================
//=========================================
var (
	mutex sync.Mutex
	serverHostPort string
	connection net.PacketConn
	destinationAddr *net.UDPAddr
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
// Client start setup
//=========================================
//=========================================

// This function connects the client process to the server
func startClient(serverHostPort string) { 
	conn, err := net.ListenPacket("udp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	connection = conn

	dst, err := net.ResolveUDPAddr("udp", serverHostPort)
	if err != nil {
		log.Fatal(err)
	}
	destinationAddr = dst
}

// Disconnects the client from the UDP connection when client exits
func exitClient() {
	// We wish to prevent client from exiting while still sending message
	mutex.Lock()
	defer mutex.Unlock()

	// Close UDP connection
	connection.Close()

	// Exit the process from OS
	os.Exit(0)
}

/* startCommandInterface runs the interactive 
 command interface for the Client Process */
func startCommandInterface() {
	reader := bufio.NewReader(os.Stdin)                                      // Create a reader for stdin
	fmt.Println("Client Process command interface started. Enter commands:") 

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
				fmt.Println("Exiting Client Process.") 
				log.Printf("[startCommandInterface] Exit command received. serverHostPort: %s", serverHostPort)
				exitClient()
			default:
				// Sends message to server
				// Check if message is in a valid format
				if cmd == "" {
					log.Printf("Command must be non-empty")
				} else if containsPunctuationWhitespace(cmd) {
					log.Printf("Command should not contain whitespaces and punctuations")
				} else if !containsValidCommandString(cmd){
					log.Printf("Command is an invalid Command string (Should contain only letters/digits/underscore/hyphens)")
				} else {
					// Valid command string, create commandName and send message across UDP to server
					var command miniraft.Raft_CommandName
					command.CommandName = cmd
					sendCommand(command)
				}
		}
	}
}

//=========================================
//=========================================
// Client Message Utilities
//=========================================
//=========================================

// Function that checks if a string contains punctuations and whitespaces
func containsPunctuationWhitespace(s string) bool {
    for _, runeValue := range s {
        if unicode.IsPunct(runeValue) || unicode.IsSpace(runeValue) {
            return true
        }
    }
    return false
}

// Function that checks if a string contains only letters/digits/underscore/hyphens
func containsValidCommandString(s string) bool {
    for _, r := range s {
        if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '-' && r != '_' {
            return false
        }
    }
    return true
}

//=========================================
//=========================================
// Client Communication
//=========================================
//=========================================

// Sends a message through the UDP connection to the server
func sendCommand(command miniraft.Raft_CommandName) {
	// Lock to prevent any other server action while sending command
	mutex.Lock()
	defer mutex.Unlock()

	// Sends command through UDP connection
	_, err := connection.WriteTo([]byte(command.CommandName), destinationAddr)
	if err != nil {
		log.Fatal(err)
	}	
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
	if len(os.Args) != 2 {
		fmt.Println("Client Usage: go run raftclient.go <server-host:server-port>")
		return
	}

	serverHostPort := os.Args[1] // Get the server address from command-line arguments
	if !strings.Contains(serverHostPort, ":") {
		fmt.Println("Invalid server address. Must be in the format host:port.") // Validate the registry address format
		return
	}

	// Debugging statement to check the initial value of serverHostPort
	log.Printf("[main] raftclient connecting to serverHostPort: %s", serverHostPort)

	// Sets the server's HostPort value for this client
	setServerHostPort(os.Args[1])

	// Tries to connect client to the defined server
	go startClient(getServerHostPort())

	// Start the interactive command interface
	startCommandInterface() 
}