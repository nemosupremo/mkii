package main

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"time"
)

func StringMsg(s string) []byte {
	b := make([]byte, 4+len(s))
	binary.LittleEndian.PutUint32(b, uint32(len(s)))
	copy(b[4:], s)
	return b
}

type HelloCmd struct{}

func (_ HelloCmd) Cmd() []byte {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, 9999)
	return b
}

type JoinCmd struct {
	Name string
}

func (j JoinCmd) Cmd() []byte {
	msg := StringMsg(j.Name)
	b := make([]byte, 2+len(msg))
	binary.LittleEndian.PutUint16(b, 1001)
	copy(b[2:], msg)
	return b
}

func main() {
	rand.Seed(time.Now().Unix())
	c, err := net.Dial("tcp", "localhost:6379")
	if err != nil {
		panic("Failed to dial.")
	}
	defer c.Close()
	// c.Write(HelloCmd{}.Cmd())
	// c.Write(JoinCmd{"rust"}.Cmd())
	c.Write([]byte(":100\r\n"))
	c.Write([]byte("+OK\r\n"))
	c.Write([]byte("-BAD\r\n"))

	for i := byte(0); i < 3; i++ {
		c.Write([]byte("+OK" + hex.EncodeToString([]byte{i}) + "\r\n"))
	}

	c.Write([]byte("$6\r\nfoobar\r\n"))
	bx := make([]byte, 4*1024*1024)
	rand.Read(bx)
	// fmt.Println("$" + strconv.Itoa(len(bx)) + "\r\n")
	c.Write([]byte("$" + strconv.Itoa(len(bx)) + "\r\n"))
	c.Write(bx)
	c.Write([]byte("\r\n"))
	//c.Write([]byte("goodbyte world!"))
	hs := md5.Sum(bx)
	c.Write([]byte("*3\r\n:1\r\n:2\r\n:3\r\n"))
	c.Write([]byte("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
	c.Write([]byte("*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n"))
	c.Write([]byte("*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n"))
	fmt.Printf("Sent data (%s)\n", hex.EncodeToString(hs[:]))
	b := make([]byte, 32)
	for {
		if n, err := c.Read(b); err == nil {
			fmt.Printf("[%d]: %s (%s)\n", n, string(b[:n]), hex.EncodeToString(b[:n]))
		} else {
			break
		}
	}
	c.Close()
}
