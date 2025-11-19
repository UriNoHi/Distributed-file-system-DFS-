package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)


func main() {
	// Listen on TCP port 8001
	cmd := os.Args[1]

	ip_port := "localhost:" + cmd
	socket, err := net.Listen("tcp", ip_port)
	if err != nil {
		fmt.Println("Error starting TCP server:", err)
		return
	}
	defer socket.Close()
	fmt.Println("Server is listening on port 8001...")

	for {
		// Accept a connection
		coneccion, err := socket.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		//**************************************
		// CONECCION ESTABLECIDA
		//**************************************
		fmt.Println("Client connected:", coneccion.RemoteAddr())

		// Handle the connection in a new goroutine
		go handleConnection(coneccion)
	}
}

func handleConnection(coneccion net.Conn) {
	defer coneccion.Close()
	
	// Crear lector para leer del cliente
	reader := bufio.NewReader(coneccion)
	for {
		// Leer mensaje del cliente (hasta \n)
		comando, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Cliente desconectado:", coneccion.RemoteAddr())
			return
		}


		fmt.Printf("Comando recibido de Cliente: %s", comando)
		
		parts := strings.SplitN(strings.TrimSpace(comando), " ", 3)

		switch parts[0] {
		case "store":
			store(parts[1], parts[2])
		case "read":

		default:
			fmt.Println("DEFAULT")
		}

		// Responder al cliente
		coneccion.Write([]byte("Mensaje recibido: " + comando))
	}
}	

func store(filename string, data string) {
		//creo un archivo y lo guardo en la carpeta blocks/
		fmt.Println("STORE en Datanode:", filename)


		file, err := os.Create("blocks/" + filename)
		if err != nil {
			fmt.Println("Error creando archivo:", err)
			return
		}
		file.WriteString(data)
		fmt.Println("Archivo guardado:", filename)
		defer file.Close()
}