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
	fmt.Println("Server is listening on port ", cmd)

	for {
		coneccion, err := socket.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		fmt.Println("Client connected:", coneccion.RemoteAddr())

		go handleConnection(coneccion)
	}
}

func handleConnection(coneccion net.Conn) {
	defer coneccion.Close()
	// Crear lector para leer del cliente
	reader := bufio.NewReader(coneccion)
	for {
		// Leer mensaje del cliente (hasta \n)
		argumentos, err := reader.ReadString('\n')
		//n, err := io.ReadFull(reader, make([]byte, 1024))
		if err != nil {
			fmt.Println("Cliente desconectado:", coneccion.RemoteAddr())
			return
		}
		if len(argumentos) == 0 {
			continue
		}

		fmt.Printf("Comando recibido de Cliente: %s", argumentos)

		parts := strings.Split(strings.TrimSpace(string(argumentos)), " ")

		if len(parts) < 2 {
			fmt.Println("Argumentos inválido recibido:", argumentos)
			return
		}

		cmd := parts[0]
		fileName := parts[1]

		switch cmd {
		case "store":
			blockSizeSTR := parts[2]
			blockSize, _ := strconv.Atoi(blockSizeSTR)
			//fmt.Println("Partes del comando: ", parts)

			buffer := make([]byte, blockSize)
			_, err = io.ReadFull(reader, buffer)
			if err != nil {
				fmt.Println("Error al leer bloque de datos:", err)
				return
			}
			//fmt.Printf("Bloque de datos recibido, %s\n", buffer)
			store(fileName, buffer)
		case "read":
			read(parts[1], coneccion)
		default:
			fmt.Println("DEFAULT")
		}
	}
}

func store(filename string, data []byte) {
	//creo un archivo y lo guardo en la carpeta blocks/
	fmt.Println("	==> STORE en Datanode:", filename)

	file, err := os.Create("blocks/" + filename)
	if err != nil {
		fmt.Println("Error creando archivo:", err)
		return
	}
	file.WriteString(string(data))
	fmt.Println("	====> Archivo guardado:", filename)
	defer file.Close()
}

func read(filename string, coneccion net.Conn) {
	//abro el archivo de la carpeta blocks/
	fmt.Println("READ en Datanode:", filename)
	file, err := os.Open("blocks/" + filename)
	if err != nil {
		fmt.Println("\nError abriendo archivo:", err)
		return
	}
	defer file.Close()

	buffer := make([]byte, 1024) // 1KB
	//leo el contenido

	n, err := file.Read(buffer)
	if err != nil && err != io.EOF {
		fmt.Println("Error leyendo:", err)
		return
	}

	// Primero envío el tamaño del bloque
	size := []byte(fmt.Sprintf("%d\n", n))
	coneccion.Write(size)

	// Luego envío exactamente los bytes leídos
	coneccion.Write(buffer[:n])
}
