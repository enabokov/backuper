package service

import (
	"fmt"
	"google.golang.org/grpc"
	"net"
	"regexp"
	"strings"

	"github.com/enabokov/backuper/internal/log"
)

func GetPrivateIP() (privateIP string) {
	ifaces, err := net.Interfaces()
	if err != nil {
		log.Error.Println(err)
		return ""
	}

	var ethIfaces []net.Interface
	for _, iface := range ifaces {
		if strings.HasPrefix(iface.Name, `en`) || strings.HasPrefix(iface.Name, `eth`) {
			ethIfaces = append(ethIfaces, iface)
		}
	}

	var cidr string
	for _, iface := range ethIfaces {
		addrs, _ := iface.Addrs()
		for _, addr := range addrs {
			matched, err := regexp.Match(`^(\d{1,3}|\.){4}`, []byte(addr.String()))
			if err != nil {
				log.Error.Println(err)
			}

			if matched {
				cidr = addr.String()
				break
			}
		}
	}

	privateIP = strings.Split(cidr, `/`)[0]
	return privateIP
}

func run(server *grpc.Server, port int) error {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Error.Println(err)
		return err
	}

	log.Info.Println("Listening on ...", port)
	if err := server.Serve(l); err != nil {
		log.Error.Printf("failed to listen: %+v\n", err)
		return err
	}

	return nil
}

func Run(server *grpc.Server, port int) error {
	return run(server, port)
}
