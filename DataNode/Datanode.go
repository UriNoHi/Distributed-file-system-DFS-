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

func main() {
	cmd := os.Args[1]
	setupLog()
	log.Println("Iniciando Datanode en el puerto ", cmd)

	ip_port := "localhost:" + cmd

	socket, err := net.Listen("tcp", ip_port)
	if err != nil {
		log.Println("[ERROR] Error starting TCP server:", err)
		return
	}
	defer socket.Close()
	fmt.Println("Datanode is listening on port ", cmd)
	log.Println("Datanode is listening on port ", cmd)

	for {
		coneccion, err := socket.Accept()
		if err != nil {
			log.Println("[ERROR] Error accepting connection:", err)
			continue
		}
		log.Println("[INFO] Client connected:", coneccion.RemoteAddr())

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
			log.Println("[WARNING] Cliente desconectado:", coneccion.RemoteAddr())
			return
		}
		if len(argumentos) == 0 {
			continue
		}

		log.Printf("[INFO] Comando recibido de Cliente: %s", argumentos)

		parts := strings.Split(strings.TrimSpace(string(argumentos)), " ")

		if len(parts) < 2 {
			//log.Println("Argumentos inválido recibido:", argumentos)
			return
		}

		cmd := parts[0]
		fileName := parts[1]

		switch cmd {
		case "store":
			blockSizeSTR := parts[2]
			blockSize, _ := strconv.Atoi(blockSizeSTR)
			log.Println("[INFO] Partes del comando: ", parts)

			buffer := make([]byte, blockSize)
			_, err = io.ReadFull(reader, buffer)
			if err != nil {
				log.Println("[ERROR] Error al leer bloque de datos:", err)
				return
			}

			store(fileName, buffer)
		case "read":
			read(parts[1], coneccion)

		case "rm":
			remove(fileName)
		default:
			log.Println("DEFAULT")
		}
	}
}

func store(filename string, data []byte) {
	//creo un archivo y lo guardo en la carpeta blocks/
	log.Println("[INFO]	==> STORE en Datanode:", filename)

	file, err := os.Create("blocks/" + filename)
	if err != nil {
		log.Println("[ERROR] Error creando archivo:", err)
		return
	}
	file.WriteString(string(data))
	log.Println("[INFO]	====> Archivo guardado:", filename)
	defer file.Close()
}

func read(filename string, coneccion net.Conn) {
	//abro el archivo de la carpeta blocks/
	log.Println("[INFO] READ en Datanode:", filename)
	file, err := os.Open("blocks/" + filename)
	if err != nil {
		log.Println("\n[ERROR] Error abriendo archivo:", err)
		return
	}
	defer file.Close()

	buffer := make([]byte, 1024) // 1KB
	//leo el contenido

	n, err := file.Read(buffer)
	if err != nil && err != io.EOF {
		log.Println("[ERROR] Error leyendo:", err)
		return
	}

	// Primero envío el tamaño del bloque
	size := []byte(fmt.Sprintf("%d\n", n))
	coneccion.Write(size)

	// Luego envío exactamente los bytes leídos
	coneccion.Write(buffer[:n])
}

func setupLog() {
	file, err := os.OpenFile("datanode.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("No se pudo abrir archivo de log: %v", err)
	}

	mw := io.MultiWriter(os.Stdout, file)

	log.SetOutput(mw)
	log.SetFlags(log.LstdFlags | log.Lshortfile) // fecha, hora y línea de código
}

func remove(fileName string) {
	log.Println("[INFO] RM en Datanode:", fileName)
	err := os.Remove("blocks/" + fileName)
	if err != nil {
		log.Println("[ERROR] Error eliminando archivo:", err)
		return
	}
	log.Println("[INFO] Archivo eliminado:", fileName)
}
