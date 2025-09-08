package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	defer l.Close()
	fmt.Println("started listening to 9092")
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		// Read the message size (4 bytes)
		messageSizeBytes := make([]byte, 4)
		_, err := io.ReadFull(conn, messageSizeBytes)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error reading message size: %v\n", err)
			break
		}

		// Parse message size (big-endian)
		messageSize := binary.BigEndian.Uint32(messageSizeBytes)
		fmt.Printf("Message size: %d\n", messageSize)

		// Read the rest of the message
		messageBytes := make([]byte, messageSize)
		_, err = io.ReadFull(conn, messageBytes)
		if err != nil {
			fmt.Printf("Error reading message body: %v\n", err)
			break
		}

		// Parse request header v2
		correlationID, err := parseRequestHeaderV2(messageBytes)
		if err != nil {
			fmt.Printf("Error parsing request header: %v\n", err)
			break
		}

		fmt.Printf("Correlation ID: %d\n", correlationID)

		// Build response: message_size (4 bytes) + correlation_id (4 bytes)
		response := make([]byte, 8)
		// message_size = 4 (for the correlation_id field)
		binary.BigEndian.PutUint32(response[0:4], 4)
		// correlation_id
		binary.BigEndian.PutUint32(response[4:8], correlationID)

		_, err = conn.Write(response)
		if err != nil {
			fmt.Printf("Error writing response: %v\n", err)
			break
		}

		// Close connection after sending response for this stage
		break
	}
}

// parseRequestHeaderV2 parses a Kafka request header v2 and returns the correlation_id
// Request header v2 structure:
// - request_api_key (INT16, 2 bytes)
// - request_api_version (INT16, 2 bytes)
// - correlation_id (INT32, 4 bytes)
// - client_id (NULLABLE_STRING, variable length)
// - TAG_BUFFER (COMPACT_ARRAY, variable length)
func parseRequestHeaderV2(data []byte) (uint32, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("insufficient data for request header v2")
	}

	// Skip request_api_key (2 bytes) and request_api_version (2 bytes)
	// correlation_id starts at offset 4
	correlationID := binary.BigEndian.Uint32(data[4:8])

	return correlationID, nil
}
