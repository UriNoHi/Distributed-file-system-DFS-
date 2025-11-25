package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

var conn net.Conn
var err error
var reader *bufio.Reader
var readerCommand *bufio.Reader

func main() {
	namenode := "localhost:8080"
	if len(os.Args) < 2 {
		log.Println("[WARN] No se proporcionó la dirección del Namenode. Usando por defecto: ", namenode)
	} else{
		namenode := os.Args[1] //ip:puerto del namenode
		log.Println("[INFO] Se proporcionó la dirección del Namenode: ", namenode)
	}
	namenodeAddr := strings.TrimSpace(os.Args[1])
	setupLog()

	conn, err = net.Dial("tcp", namenodeAddr)
	if err != nil {
		log.Println("[ERROR] No se pudo conectar al Namenode: \n", err)
		os.Exit(1)
	}
	defer conn.Close()

	log.Printf("Conectado al Namenode %s\n", namenode)

	readerCommand = bufio.NewReader(os.Stdin)
	for {
		fmt.Print("DFS> ")

		input, _ := readerCommand.ReadString('\n')
		input = strings.TrimSpace(input)
		splitCommand := strings.Split(input, " ")

		if len(splitCommand) == 0 {
			input = "DEFAULT"
		}
		log.Println("Comando ingresado:", input)

		switch splitCommand[0] {
		case "put":
			if len(splitCommand) < 2 {
				usage("put")
			}
			put(splitCommand[1])

		case "get":
			// usage: get <remote-path>
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
			if len(splitCommand) > 1 {
				usage("ls")
			}
			ls()

		case "rm":
			if len(splitCommand) < 2 {
				usage("rm")
			}
			rm(splitCommand[1])

		case "exit":
			log.Println("Cerrando cliente...")
			return

		default:
			log.Printf("unknown command: %s\n", input)
			usage("")

		}
	}
}

func usage(cmd string) {
	switch cmd {
	case "put":
		log.Println("uso del comando: put <local-file>")

	case "get":
		log.Println("uso del comando: get <local-file> ")

	case "info":
		log.Println("uso del comando: info <local-file>")

	case "ls":
		log.Println("uso del comando: ls , sin argumentos")

	default:
		log.Println("Usage:")
		log.Println("  put <local-path>    Upload a file")
		log.Println("  get <remote-path>   Download a file")
		log.Println("  info <path>         Show info about a file")
		log.Println("  ls                  List files in the metadata")
	}

}

func put(fileName string) {
	log.Println("Ejecutando comando put con argumentos:", fileName)

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
	log.Println("Ejecutando comando get con argumentos:", fileName)

	toSend := "get " + fileName + "\n"
	sendToNamenode(toSend)
	response := responseFromNamenode()

	listOfDataNodes := strings.Split(response, ",")
	log.Println("Lista de DataNodos: ", listOfDataNodes)

	buffer := readDataNodes(listOfDataNodes, fileName)

	createLocalFile(buffer, fileName)
}

func info(file string) {
	log.Println("Ejecutando comando info con argumentos:", file)
	sendToNamenode("info " + file + "\n")
	response := responseFromNamenode()

	log.Println(" ===== Información del archivo: " + file + " ===== ")
	//quiero separarlos por coma y mostrarlos en líneas separadas
	splitInfo := strings.Split(response, ",")
	for i, info := range splitInfo {
		toPrint := "Bloque " + strconv.Itoa(i) + " en datanode: " + info
		log.Println(toPrint)
	}
}

func ls() {
	log.Println("Ejecutando comando ls")
	sendToNamenode("ls\n")
	response := responseFromNamenode()
	log.Println(" ===== Contenido del metadata ===== ")
	splitFiles := strings.Split(response, ",")
	for _, file := range splitFiles {
		log.Println("-	", file)
	}
}

func rm(fileName string) {
	log.Println("Ejecutando comando rm")
	sendToNamenode("rm " + fileName + "\n")
	response := responseFromNamenode()
	listOfDataNodes := strings.Split(response, ",")
	removeDataNodes(listOfDataNodes, fileName)
	log.Println("Archivo eliminado del DFS: ", fileName)
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
			log.Println("[ERROR] Error leyendo:", err)
			break
		}
		if blockSize == 0 {
			break
		}

		block := make([]byte, blockSize)
		copy(block, buffer[:blockSize])
		log.Printf("\n Bloque %d leído, tamaño %d \n ========================================", cantBlocks, blockSize)

		log.Println("[DEBUG] Tamaño esperado:", blockSize)
		log.Println("[DEBUG] Tamaño recibido real:", len(block))

		buffers = append(buffers, block)
		cantBlocks++
	}
	return buffers, cantBlocks
}

func sendToNamenode(message string) {
	log.Println("\nComando que mando a Namenode: ", message)
	_, err = conn.Write([]byte(message))
	if err != nil {
		log.Println("[ERROR] Error al enviar:", err)
		return
	}
}

func responseFromNamenode() string {
	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		log.Println("[ERROR] Error al recibir respuesta:", err)
		os.Exit(1)
	}
	log.Println("Respuesta del Namenode: \n", response)
	return response
}

func storeDataNodes(dataNodes []string, buffers [][]byte, fileName string, cantBlocks int) {

	for i := 0; i < cantBlocks; i++ {
		dnAddress := strings.TrimSpace(dataNodes[i])
		log.Printf("Enviando bloque %d al Datanode %s\n", i, dnAddress)

		dataNode, err := net.Dial("tcp", dnAddress)
		if err != nil {
			log.Println("[ERROR] Error al conectar con el Datanode:", err)
			return
		}
		//defer dataNode.Close()

		//Primero envio argumentos
		argumentos := "store " + fileName + "_block_" + fmt.Sprint(i) + " " + strconv.Itoa(len(buffers[i])) + "\n"
		dataNode.Write([]byte(argumentos))

		//Luego envio el bloque de datos
		//toSave := string(buffers[i]) //+ "\n"
		dataNode.Write(buffers[i])

		log.Printf("Bloque %d enviado al Datanode \n", i)

		dataNode.Close()
	}
	for i := cantBlocks; i < (cantBlocks * 2); i++ {
		//Envio al Backup
		indexNode := i - cantBlocks
		dnBackupAddress := strings.TrimSpace(dataNodes[i])
		log.Printf("Enviando bloque de recuperacion %d al Datanode %s\n", indexNode, dnBackupAddress)

		dataBackupNode, err := net.Dial("tcp", dnBackupAddress)
		if err != nil {
			log.Println("[ERROR] Error al conectar con el Datanode:", err)
			return
		}
		defer dataBackupNode.Close()

		//Primero envio argumentos
		argumentosB := "store " + fileName + "_backup_block_" + fmt.Sprint(indexNode) + " " + strconv.Itoa(len(buffers[indexNode])) + "\n"
		dataBackupNode.Write([]byte(argumentosB))

		//Luego envio el bloque de datos
		toSave := string(buffers[indexNode]) + "\n"
		dataBackupNode.Write([]byte(toSave))

		log.Printf("Bloque %d enviado al Datanode \n", indexNode)
		dataBackupNode.Close()
	}
}

func readDataNodes(dataNodes []string, fileName string) []byte {
	buffer := make([]byte, 1024) // 1KB
	//limpiar buffer antes de usar
	buffer = []byte{}
	for i, dn := range dataNodes {
		dnAddress := strings.TrimSpace(dn)
		log.Printf("Conectando al Datanode %s para leer el bloque %s\n", dnAddress, strconv.Itoa(i))
		dataNode, err := net.Dial("tcp", dnAddress)
		if err != nil {
			log.Println("[ERROR] Error al conectar con el Datanode:", err)
			//os.Exit(1)
			recuperateFromAnotherNode(i, dataNodes, fileName, &buffer)
			continue
		}
		defer dataNode.Close()

		toRead := "read " + fileName + "_block_" + strconv.Itoa(i) + "\n"

		log.Println("\nComando que mando a Datanode: ", toRead)
		dataNode.Write([]byte(toRead))

		reader = bufio.NewReader(dataNode)

		sizeStr, err := reader.ReadString('\n')
		if err != nil {
			log.Println("[ERROR] Error al leer tamaño del bloque:", err)
			return nil
		}

		sizeStr = strings.TrimSpace(sizeStr)
		blockSize, _ := strconv.Atoi(sizeStr)

		block := make([]byte, blockSize)
		var n int
		n, err = io.ReadFull(reader, block)
		if err != nil {
			log.Println("[ERROR] Error al leer bloque:", err)
			return nil
		}
		log.Printf("[DEBUG] Tamaño recibido de  ReadFull: ", n)

		log.Printf("[DEBUG] recibido %d bytes, tamaño declarado %d", len(block), blockSize)
		if len(block) != blockSize {
			log.Println("[WARN] mismatch tamaño")
		}

		log.Printf("[DEBUG] primeros bytes: %x", block[:min(16, len(block))])

		buffer = append(buffer, block...)

		//log.Printf("Bloque recibido del Datanode %s: %s\n", dnAddress, string(block))

	}
	//log.Printf("Archivo completo recibido: %s\n", string(buffer))

	return buffer
}

func createLocalFile(buffer []byte, fileName string) {
	localFile, err := os.Create(fileName)
	if err != nil {
		log.Println("[ERROR] Error creando archivo local:", err)
		return
	}
	defer localFile.Close()

	if err := os.WriteFile(localFile.Name(), buffer, 0644); err != nil {
		log.Println("[ERROR] Error writing file:", err)
	}
}

func recuperateFromAnotherNode(failedIndex int, dataNodes []string, fileName string, buffer *[]byte) {
	log.Println("Recuperando bloque desde otro Datanode...")
	for i, dn := range dataNodes {
		if i == failedIndex {
			continue
		}
		dnAddress := strings.TrimSpace(dn)
		log.Printf("Intentando leer bloque desde el Datanode %s\n", dnAddress)
		dataNode, err := net.Dial("tcp", dnAddress)
		if err != nil {
			log.Println("[ERROR] Error al conectar con el Datanode:", err)
			continue
		}
		defer dataNode.Close()

		toRead := "read " + fileName + "_backup_block_" + strconv.Itoa(failedIndex) + "\n"
		log.Println("\n[RECOVER] Comando que mando a Datanode: ", toRead)
		dataNode.Write([]byte(toRead))
		reader = bufio.NewReader(dataNode)

		sizeStr, err := reader.ReadString('\n')
		if err != nil {
			log.Println("[ERROR] Error al leer tamaño del bloque:", err)
			return
		}
		sizeStr = strings.TrimSpace(sizeStr)
		blockSize, _ := strconv.Atoi(sizeStr)
		block := make([]byte, blockSize)
		_, err = io.ReadFull(reader, block)
		if err != nil {
			log.Println("[ERROR] Error al leer bloque:", err)
			return
		}
		*buffer = append(*buffer, block...)
		log.Printf("Bloque recuperado del Datanode %s: %s\n", dnAddress, string(block))
		return
	}
	log.Println("No se pudo recuperar el bloque desde ningún Datanode.")
}

func setupLog() {
	file, err := os.OpenFile("Cliente.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("[ERROR] No se pudo abrir archivo de log: %v", err)
	}

	mw := io.MultiWriter(os.Stdout, file)

	log.SetOutput(mw)
	log.SetFlags(log.LstdFlags | log.Lshortfile) // fecha, hora y línea de código
}

func removeDataNodes(dataNodes []string, fileName string) {
	for i, dn := range dataNodes {
		dnAddress := strings.TrimSpace(dn)
		log.Printf("Conectando al Datanode %s para eliminar los bloques\n", dnAddress)
		dataNode, err := net.Dial("tcp", dnAddress)
		if err != nil {
			log.Println("[ERROR] Error al conectar con el Datanode:", err)
			return
		}
		defer dataNode.Close()

		toDelete := "rm " + fileName + "_block_" + strconv.Itoa(i) + "\n"
		log.Println("\nComando que mando a Datanode: ", toDelete)
		dataNode.Write([]byte(toDelete))
		dataNode.Close()
	}
}
