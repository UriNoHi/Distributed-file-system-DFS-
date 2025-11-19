package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
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

	cmd := os.Args[1]
	switch cmd {
	case "put":
		if len(os.Args) < 3 {
			usage("put")
		}
		put()

	case "get":
		// usage: get <remote-path> <local-path>
		if len(os.Args) < 3 {
			usage("get")
		}
		get()

	case "info":
		// usage: info <path>
		if len(os.Args) < 3 {
			usage("info")
		}

	case "ls":
		// usage: ls [path]
		if len(os.Args) >= 3 {
			usage("ls")
		}

	default:
		fmt.Printf("unknown command: %s\n", cmd)
		usage("")
	}
}

func usage(cmd string) {
	fmt.Println("Usage:")
	fmt.Println("  put <local-path> <remote-path>   Upload a file")
	fmt.Println("  get <remote-path> <local-path>   Download a file")
	fmt.Println("  info <path>                      Show info about a file")
	fmt.Println("  ls [path]                        List directory")

	switch cmd {
	case "put":
		fmt.Println("usage: put <local-file> <remote-path>")

	case "get":
		fmt.Println("usage: get <remote-path> <local-path>")

	case "info":
		fmt.Println("usage: info <path>")

	case "ls":
		fmt.Println("usage:")

	default:
		fmt.Println("Usage:")
		fmt.Println("  put <local-path> <remote-path>   Upload a file")
		fmt.Println("  get <remote-path> <local-path>   Download a file")
		fmt.Println("  info <path>                      Show info about a file")
		fmt.Println("  ls [path]                        List directory")
	}
	os.Exit(1)

}

func put() {
	fmt.Println("Ejecutando comando put con argumentos:", os.Args[1], os.Args[2])
	//Abro el archivo local

	file := abrirArchivoLocal()
	if file == nil {
		return
	}

	//Lo parto en bloques de 1KB
	buffers := [][]byte{}
	cantBlocks := 0
	buffers, cantBlocks = particionarArchivoEnBloques(file)

	//Consulta al Namenode dónde guardar cada bloque

	toSend := os.Args[1] + " " + os.Args[2] + " " + fmt.Sprint(cantBlocks) + "\n"
	_, err = conn.Write([]byte(toSend))
	if err != nil {
		fmt.Println("Error al enviar:", err)
		return
	}

	//Recibe la lista de Datanodes asignados. (response)
	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error al recibir respuesta:", err)
		return
	}
	fmt.Println("Respuesta del Namenode: \n", response)

	//Enviar los bloques a los Datanodes asignados
	dataNodes := strings.Split(response, ",")

	for i := 0; i < cantBlocks; i++ {
		dnAddress := strings.TrimSpace(dataNodes[i])
		fmt.Printf("Enviando bloque %d al Datanode %s\n", i, dnAddress)

		dataNode, err := net.Dial("tcp", dnAddress)
		if err != nil {
			fmt.Println("Error al conectar con el Datanode:", err)
			return
		}
		defer dataNode.Close()

		toSave := "store " + os.Args[2] + "_block_" + fmt.Sprint(i) + " " + string(buffers[i]) + "\n"

		dataNode.Write([]byte(toSave))

		fmt.Printf("Bloque %d enviado al Datanode \n", i)
	}
}

func get() {
	fmt.Println("Ejecutando comando get con argumentos:", os.Args[1])

	toSend := os.Args[1] + " " + os.Args[2] + "\n"
	_, err = conn.Write([]byte(toSend))
	if err != nil {
		fmt.Println("Error al enviar:", err)
		return
	}

	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error al recibir respuesta:", err)
		return
	}
	listOfDataNodes := strings.Split(response, ",")
	for _, dn := range listOfDataNodes {
		fmt.Printf("Datanode: %s\n", dn)
	}

}

func info(path string) {
	// TODO: implement info logic
	fmt.Printf("INFO: path=%s\n", path)
}

func ls(path string) {
	// TODO: implement listing logic
	fmt.Printf("LS: path=%s\n", path)
}

func abrirArchivoLocal() *os.File {
	file, err := os.Open(os.Args[2])
	if err != nil {
		fmt.Fprintf(os.Stderr, "no se pudo abrir el archivo %s: %v\n", os.Args[2], err)
		os.Exit(1)
	}
	defer file.Close()
	return file
}

func particionarArchivoEnBloques(file *os.File) ([][]byte, int) {
	buffers := [][]byte{}
	cantBlocks := 0
	buffer := make([]byte, 1024) // 1KB
	for {
		n, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			fmt.Println("Error leyendo:", err)
			break
		}
		if n == 0 {
			break
		}

		block := make([]byte, n)
		copy(block, buffer[:n])
		fmt.Printf("\n Bloque %d leído, tamaño %d bytes contenido:\n %s \n ========================================", cantBlocks, n, string(block))
		buffers = append(buffers, block)
		cantBlocks++
	}
	return buffers, cantBlocks
}
