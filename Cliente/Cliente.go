package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

var conn net.Conn
var err error
var reader *bufio.Reader

func main() {

	namenode := "localhost:8080"
	//reader = bufio.NewReader(conn)

	conn, err = net.Dial("tcp", namenode)
	if err != nil {
		fmt.Fprintf(os.Stderr, "no se pudo conectar al Namenode %s: %v\n", namenode, err)
		os.Exit(1)
	}
	defer conn.Close()

	// Conn se mantiene abierta durante la ejecución; si necesitas usarla en otras
	// funciones exporta una variable global o pásala como argumento.
	fmt.Printf("Conectado al Namenode %s\n", namenode)
	reader = bufio.NewReader(os.Stdin)
	for {
		fmt.Print("DFS> ")

		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		splitCommand := strings.Split(input, " ")

		if len(splitCommand) == 0 {
			input = ""
		}

		switch splitCommand[0] {
		case "put":
			if len(splitCommand) < 2 {
				usage("put")
			}
			put(splitCommand[1])

		case "get":
			// usage: get <remote-path> <local-path>
			if len(splitCommand) < 2 {
				usage("get")
			}
			get(splitCommand[1])

		case "info":
			// usage: info <path>
			if len(splitCommand) < 2 {
				usage("info")
			}
			info(splitCommand[1])

		case "ls":
			// usage: ls [path]
			if len(splitCommand) >= 1 {
				usage("ls")
			}
			ls()

		case "exit":
			fmt.Println("Cerrando cliente...")
			return

		default:
			fmt.Printf("unknown command: %s\n", input)
			usage("")
		}
	}
}

func usage(cmd string) {

	switch cmd {
	case "put":
		fmt.Println("uso del comando: put <local-file>")

	case "get":
		fmt.Println("uso del comando: get <local-file> ")

	case "info":
		fmt.Println("uso del comando: info <local-file>")

	case "ls":
		fmt.Println("uso del comando: ls , sin argumentos")

	default:
		fmt.Println("Usage:")
		fmt.Println("  put <local-path> <remote-path>   Upload a file")
		fmt.Println("  get <remote-path> <local-path>   Download a file")
		fmt.Println("  info <path>                      Show info about a file")
		fmt.Println("  ls [path]                        List directory")
	}

}

func put(fileName string) {
	fmt.Println("Ejecutando comando put con argumentos:", fileName)

	//Abro el archivo local
	file := abrirArchivoLocal(fileName)
	if file == nil {
		return
	}

	//Lo parto en bloques de 1KB
	buffers := [][]byte{}
	cantBlocks := 0

	buffers, cantBlocks = particionarArchivoEnBloques(file)
	defer file.Close()

	//Consulta al Namenode dónde guardar cada bloque
	toSend :=
		"put " + //comando <put>
			fileName + " " + //archivo que quiero guardar
			fmt.Sprint(cantBlocks) + //número de bloques del archivo
			"\n"
	sendToNamenode(toSend)

	//Recibe la lista de Datanodes asignados. (response)
	response := responseFromNamenode()

	//Enviar los bloques a los Datanodes asignados
	dataNodes := strings.Split(response, ",")
	storeDataNodes(dataNodes, buffers, fileName, cantBlocks)

}

func get(fileName string) {
	fmt.Println("Ejecutando comando get con argumentos:", fileName)

	toSend := "get " + fileName + "\n"
	sendToNamenode(toSend)
	response := responseFromNamenode()

	listOfDataNodes := strings.Split(response, ",")
	fmt.Println("Lista de DataNodos: ", listOfDataNodes)

	buffer := readDataNodes(listOfDataNodes, fileName)

	createLocalFile(buffer, fileName)
}

func info(file string) {
	fmt.Println("Ejecutando comando info con argumentos:", file)
	sendToNamenode("info " + file + "\n")
	response := responseFromNamenode()

	fmt.Println(" ===== Información del archivo: " + file + " ===== ")
	//quiero separarlos por coma y mostrarlos en líneas separadas
	splitInfo := strings.Split(response, ",")
	for i, info := range splitInfo {
		toPrint := "Bloque " + strconv.Itoa(i) + " en datanode: " + info
		fmt.Println(toPrint)
	}
}

func ls() {
	fmt.Println("Ejecutando comando ls")
	sendToNamenode("ls\n")
	response := responseFromNamenode()
	fmt.Println(" ===== Contenido del metadata ===== ")
	splitFiles := strings.Split(response, ",")
	for _, file := range splitFiles {
		fmt.Println("-	", file)
	}
}

func abrirArchivoLocal(nameFile string) *os.File {
	file, err := os.Open(nameFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "no se pudo abrir el archivo %s: %v\n", nameFile, err)
		os.Exit(1)
	}
	//defer file.Close()
	return file
}

func particionarArchivoEnBloques(file *os.File) ([][]byte, int) {
	buffers := [][]byte{}
	cantBlocks := 0
	buffer := make([]byte, 1024) // 1KB
	for {
		blockSize, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			fmt.Println("Error leyendo:", err)
			break
		}
		if blockSize == 0 {
			break
		}

		block := make([]byte, blockSize)
		copy(block, buffer[:blockSize])
		fmt.Printf("\n Bloque %d leído, tamaño %d \n ========================================", cantBlocks, blockSize)
		buffers = append(buffers, block)
		cantBlocks++
	}
	return buffers, cantBlocks
}

func sendToNamenode(message string) {
	fmt.Println("\nComando que mando a Namenode: ", message)
	_, err = conn.Write([]byte(message))
	if err != nil {
		fmt.Println("Error al enviar:", err)
		return
	}
}

func responseFromNamenode() string {
	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error al recibir respuesta:", err)
		os.Exit(1)
	}
	fmt.Println("Respuesta del Namenode: \n", response)
	return response
}

func storeDataNodes(dataNodes []string, buffers [][]byte, fileName string, cantBlocks int) {

	for i := 0; i < cantBlocks; i++ {
		dnAddress := strings.TrimSpace(dataNodes[i])
		fmt.Printf("Enviando bloque %d al Datanode %s\n", i, dnAddress)

		dataNode, err := net.Dial("tcp", dnAddress)
		if err != nil {
			fmt.Println("Error al conectar con el Datanode:", err)
			return
		}
		defer dataNode.Close()

		//Primero envio argumentos
		argumentos := "store " + fileName + "_block_" + fmt.Sprint(i) + " " + strconv.Itoa(len(buffers[i])) + "\n"
		dataNode.Write([]byte(argumentos))

		//Luego envio el bloque de datos
		toSave := string(buffers[i]) + "\n"
		dataNode.Write([]byte(toSave))

		fmt.Printf("Bloque %d enviado al Datanode \n", i)
		dataNode.Close()
	}
}

func readDataNodes(dataNodes []string, fileName string) []byte {
	buffer := make([]byte, 1024) // 1KB
	//limpiar buffer antes de usar
	buffer = []byte{}
	for i, dn := range dataNodes {
		dnAddress := strings.TrimSpace(dn)
		fmt.Printf("Conectando al Datanode %s para leer bloques\n", dnAddress)
		dataNode, err := net.Dial("tcp", dnAddress)
		if err != nil {
			fmt.Println("Error al conectar con el Datanode:", err)
			os.Exit(1)
		}
		defer dataNode.Close()

		toRead := "read " + fileName + "_block_" + strconv.Itoa(i) + "\n"

		fmt.Println("\nComando que mando a Datanode: ", toRead)
		dataNode.Write([]byte(toRead))

		reader = bufio.NewReader(dataNode)

		sizeStr, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error al leer tamaño del bloque:", err)
			return nil
		}

		sizeStr = strings.TrimSpace(sizeStr)
		blockSize, _ := strconv.Atoi(sizeStr)

		block := make([]byte, blockSize)
		_, err = io.ReadFull(reader, block)
		if err != nil {
			fmt.Println("Error al leer bloque:", err)
			return nil
		}

		buffer = append(buffer, block...)

		fmt.Printf("Bloque recibido del Datanode %s: %s\n", dnAddress, string(block))

	}
	fmt.Printf("Archivo completo recibido: %s\n", string(buffer))
	return buffer
}

func createLocalFile(buffer []byte, fileName string) {
	localFile, err := os.Create(fileName)
	if err != nil {
		fmt.Println("Error creando archivo local:", err)
		return
	}
	defer localFile.Close()

	if err := os.WriteFile(localFile.Name(), buffer, 0644); err != nil {
		fmt.Println("Error writing file:", err)
	}
}
