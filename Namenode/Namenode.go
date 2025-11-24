package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

type DataInfo struct {
	Block    int    `json:"block"`
	DataNode string `json:"node"`
}

var nodes = []string{}

var metadata = map[string][]DataInfo{}

func main() {
	setupLog()
	// Listen any ip and port 8080
	log.Println("Iniciando Namenode")

	socket, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Println("[ERROR] Error al iniciar el servidor TCP:", err)
		return
	}
	defer socket.Close()
	log.Println("Namenode escuchando el puerto 8080...")

	createMetadataFile()

	getNodeList()

	for {
		// Accept a connection
		coneccion, err := socket.Accept()
		if err != nil {
			log.Println("[ERROR] Error accepting connection:", err)
			continue
		}

		log.Println("Client connected:", coneccion.RemoteAddr())

		go handleConnection(coneccion)
	}
}

func handleConnection(coneccion net.Conn) {
	defer coneccion.Close()

	reader := bufio.NewReader(coneccion)
	for {
		comando, err := reader.ReadString('\n')
		if err != nil {
			log.Println("[WARNING] Cliente desconectado:", coneccion.RemoteAddr())
			return
		}

		parts := strings.Split(strings.TrimSpace(comando), " ")

		log.Printf("[INFO] Comando recibido de Cliente: %s", comando)
		log.Println("[INFO] Partes del comando: ", parts)
		switch parts[0] {
		case "put":
			cantBlocks, err := strconv.Atoi(parts[2])
			if err != nil {
				log.Println("[ERROR] Error converting block count:", err)
				return
			}
			putNameNode(parts[1], cantBlocks, coneccion)

		case "get":
			getNameNode(parts[1], coneccion)

		case "info":
			getNameNode(parts[1], coneccion)

		case "ls":
			listOfFiles(coneccion)
		case "rm":
			getNameNode(parts[1], coneccion)

		default:
			log.Println("DEFAULT")
		}

		//coneccion.Write([]byte("Mensaje recibido: " + comando))
	}
}

func putNameNode(fileName string, cantBlocks int, coneccion net.Conn) {
	log.Printf("Procesando PUT en Namenode para el archivo %s con %d bloques\n", fileName, cantBlocks)
	fmt.Printf("Procesando PUT en Namenode para el archivo %s con %d bloques\n", fileName, cantBlocks)

	listaDeDatanodes := []string{}
	if _, exists := metadata[fileName]; exists {
		log.Printf("[WARNING] El archivo %s ya existe en el sistema. Sobrescribiendo metadata.\n", fileName)
		fmt.Printf("[WARNING] El archivo %s ya existe en el sistema. Sobrescribiendo metadata.\n", fileName)
		metadata[fileName] = []DataInfo{}
	}

	for i := 0; i < cantBlocks; i++ {
		// Seleccionar un DataNode (aquí simplemente se selecciona uno al azar)
		indexNode := i % len(nodes)
		nodoSeleccionado := nodes[indexNode]

		metadata[fileName] = append(metadata[fileName], DataInfo{Block: i, DataNode: nodoSeleccionado})
		listaDeDatanodes = append(listaDeDatanodes, nodoSeleccionado)

		log.Printf("[INFO] Bloque %d del archivo %s asignado al DataNode %s\n", i, fileName, nodoSeleccionado)
		fmt.Printf("[INFO] Bloque %d del archivo %s asignado al DataNode %s\n", i, fileName, nodoSeleccionado)

	}

	for i := 0; i < cantBlocks; i++ {
		//Backup node selection
		indexNodeBackup := (i + 2) % len(nodes)
		nodoBackup := nodes[indexNodeBackup]

		metadata[fileName+"_backup"] = append(metadata[fileName], DataInfo{Block: i, DataNode: nodoBackup})
		listaDeDatanodes = append(listaDeDatanodes, nodoBackup)

		log.Printf("[INFO] Bloque de Recuperacion %d del archivo %s asignado al DataNode %s\n", i, fileName, nodoBackup)
		fmt.Printf("[INFO] Bloque de Recuperacion %d del archivo %s asignado al DataNode %s\n", i, fileName, nodoBackup)
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		log.Println("[ERROR] Error marshaling metadata:", err)
	} else {
		if err := os.WriteFile("metadata.json", data, 0644); err != nil {
			log.Println("[ERROR] Error writing metadata file:", err)
		}
	}
	_, err = coneccion.Write([]byte(strings.Join(listaDeDatanodes, ",") + "\n"))
	if err != nil {
		log.Println("[ERROR] Error al enviar:", err)
		return
	}
}

func getNameNode(fileName string, coneccion net.Conn) {
	log.Printf("[INFO] Procesando GET en Namenode para el archivo %s\n", fileName)
	fmt.Printf("[INFO] Procesando GET en Namenode para el archivo %s\n", fileName)

	listaDeDatanodes := []string{}
	if info, exists := metadata[fileName]; exists {
		for _, dataInfo := range info {
			block := dataInfo.DataNode
			listaDeDatanodes = append(listaDeDatanodes, block)
			fmt.Printf("[INFO] Bloque %d del archivo %s se encuentra en el DataNode %s\n", dataInfo.Block, fileName, block)
		}
		log.Printf("[INFO] Lista de DataNodes para el archivo %s: %v\n", fileName, listaDeDatanodes)
		_, err := coneccion.Write([]byte(strings.Join(listaDeDatanodes, ",") + "\n"))
		if err != nil {
			log.Println("[ERROR] Error al enviar:", err)

		}
	}
}

func listOfFiles(coneccion net.Conn) {
	log.Println("[INFO] Procesando LS en Namenode")
	fmt.Println("[INFO] Procesando LS en Namenode")
	listOfFiles := []string{}
	for fileName := range metadata {
		listOfFiles = append(listOfFiles, fileName)
	}
	_, err := coneccion.Write([]byte(strings.Join(listOfFiles, ",") + "\n"))
	if err != nil {
		log.Println("[ERROR] Error al enviar:", err)
	}
}

func setupLog() {
	file, err := os.OpenFile("Natanode.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("No se pudo abrir archivo de log: %v", err)
	}
	mw := io.MultiWriter(os.Stdout, file)

	log.SetOutput(mw)
	log.SetFlags(log.LstdFlags | log.Lshortfile) // fecha, hora y línea de código
}

func createMetadataFile() {
	data, err := json.MarshalIndent(metadata, "", "  ")

	if err != nil {
		log.Println("[ERROR] Error marshaling metadata:", err)
	}

	_, err = os.Stat("metadata.json")

	if err == nil {
		fileData, err := os.ReadFile("metadata.json")
		if err != nil {
			log.Println("[ERROR] Error reading metadata file:", err)
			return
		}

		err = json.Unmarshal(fileData, &metadata)
		if err != nil {
			log.Println("[ERROR] Error unmarshaling metadata file:", err)
		}
	} else if os.IsNotExist(err) {
		if err := os.WriteFile("metadata.json", data, 0644); err != nil {
			log.Println("[ERROR] Error writing metadata file:", err)

		}
		log.Println("Archivo 'metadata.json' creado correctamente ✅")
	}
}

func getNodeList() {
	log.Println("[INFO] Lista de DataNodes disponibles:")
	//leo el archivo nodeList para obtener los datanodes
	fileData, err := os.ReadFile("nodeList")
	if err != nil {
		log.Println("[ERROR] Error reading nodeList file:", err)
		return
	}
	lines := strings.Split(string(fileData), "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			nodes = append(nodes, strings.TrimSpace(line))
			log.Println("[INFO] - ", strings.TrimSpace(line))
			fmt.Println("[INFO] - ", strings.TrimSpace(line))
		}
	}
}
