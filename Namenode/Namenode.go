package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

var coneccion net.Conn

type DataInfo struct {
	Block    int    `json:"block"`
	DataNode string `json:"node"`
}

var nodes = []string{
	"localhost:8001",
	"localhost:8002",
	"localhost:8003",
}

var metadata = map[string][]DataInfo{
	"file1.txt": {
		{Block: 0, DataNode: "localhost:8001"},
		{Block: 1, DataNode: "localhost:8002"},
	},
	"file2.txt": {
		{Block: 0, DataNode: "localhost:8002"},
		{Block: 1, DataNode: "localhost:8003"},
	},
}

func main() {
	// Listen on TCP port 8080
	socket, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		fmt.Println("Error starting TCP server:", err)
		return
	}
	defer socket.Close()
	fmt.Println("Server is listening on port 8080...")

	//**********************************************************
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		fmt.Println("Error marshaling metadata:", err)
	} else {
		//fmt.Println("Metadata JSON:\n", string(data))
		if err := os.WriteFile("metadata.json", data, 0644); err != nil {
			fmt.Println("Error writing metadata file:", err)
		}
	}

	fmt.Println("Archivo 'metadata.json' creado correctamente ✅")
	//**********************************************************

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

		parts := strings.Split(strings.TrimSpace(comando), " ")

		fmt.Printf("Comando recibido de Cliente: %s", comando)
		fmt.Println("Partes del comando: ", parts)
		switch parts[0] {
		case "put":
			cantBlocks, err := strconv.Atoi(parts[2])
			if err != nil {
				fmt.Println("Error converting block count:", err)
				return
			}
			putNameNode(parts[1], cantBlocks, coneccion)

		case "get":
			getNameNode(parts[1], coneccion)

		case "ls":

		default:
			fmt.Println("DEFAULT")
		}

		// Responder al cliente
		coneccion.Write([]byte("Mensaje recibido: " + comando))
	}
}

func putNameNode(fileName string, cantBlocks int, coneccion net.Conn) {
	fmt.Printf("Procesando PUT en Namenode para el archivo %s con %d bloques\n", fileName, cantBlocks)

	listaDeDatanodes := []string{}

	for i := 0; i < cantBlocks; i++ {
		// Seleccionar un DataNode (aquí simplemente se selecciona uno al azar)
		nodoSeleccionado := nodes[i%len(nodes)]

		metadata[fileName] = append(metadata[fileName], DataInfo{Block: i, DataNode: nodoSeleccionado})
		listaDeDatanodes = append(listaDeDatanodes, nodoSeleccionado)

		fmt.Printf("Bloque %d del archivo %s asignado al DataNode %s\n", i, fileName, nodoSeleccionado)
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		fmt.Println("Error marshaling metadata:", err)
	} else {
		//fmt.Println("Metadata JSON:\n", string(data))
		if err := os.WriteFile("metadata.json", data, 0644); err != nil {
			fmt.Println("Error writing metadata file:", err)
		}
	}
	//fmt.Println("Envio la lista de nodos:", metadata)
	_, err = coneccion.Write([]byte(strings.Join(listaDeDatanodes, ",") + "\n"))
	if err != nil {
		fmt.Println("Error al enviar:", err)
		return
	}
}

func getNameNode(fileName string, coneccion net.Conn) {
	fmt.Printf("Procesando GET en Namenode para el archivo %s\n", fileName)
	listaDeDatanodes := []string{}
	if info, exists := metadata[fileName]; exists {
		for _, dataInfo := range info {
			block := dataInfo.DataNode
			listaDeDatanodes = append(listaDeDatanodes, block)
		}
		fmt.Printf("Lista de DataNodes para el archivo %s: %v\n", fileName, listaDeDatanodes)
		_, err := coneccion.Write([]byte(strings.Join(listaDeDatanodes, ",") + "\n"))
		if err != nil {
			fmt.Println("Error al enviar:", err)
			return
		}
	}
}
