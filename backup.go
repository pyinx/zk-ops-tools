package main

import (
	"flag"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	zkHost    = flag.String("host", "127.0.0.1", "hostname or ipaddr")
	zkPort    = flag.Int("port", 2181, "port")
	zkPath    = flag.String("path", "/", "zk path, eg: /zk/codis")
	zkTimeout = flag.Int("timeout", 3, "connect zk timeout")
	zkDir     = flag.String("dir", "/tmp", "backup data dir")
	// zkQps     = flag.Int("qps", 1000, "read qps: 10 - 5000 ")
)

type Qps struct {
	sync.Mutex
	Num int
}

var (
	storeFile string
	bakDir    string
	zkServer  []string
	f         *os.File
	qps       = Qps{Num: 0}
	maxQps    = 100
)

func init() {
	flag.Parse()
	zkServer = []string{fmt.Sprintf("%s:%d", *zkHost, *zkPort)}

	if !strings.HasPrefix(*zkPath, "/") {
		panic("zk path must start with /")
	}
	if strings.HasSuffix(*zkDir, "/") {
		bakDir = fmt.Sprintf("%s", *zkDir)
	} else {
		bakDir = fmt.Sprintf("%s/%s", *zkDir)
	}
	err := os.MkdirAll(bakDir, os.ModePerm)
	if err != nil {
		panic("create bak dir err: " + err.Error())
	}
	now := time.Now()
	timestamp := now.Format("20060102-1504")
	storeFile = fmt.Sprintf("%s/%s.txt", bakDir, timestamp)

}

type ZkCli struct {
	C *zk.Conn
}

func NewZkCli(host []string, timeout time.Duration) *ZkCli {
	conn, _, err := zk.Connect(host, timeout)
	if err != nil {
		panic("failed to connect zookeeper: " + err.Error())
	}
	return &ZkCli{C: conn}
}

func (cli *ZkCli) Close() {
	cli.C.Close()
}

func (cli *ZkCli) Dump(path string) {
	data, _, err := cli.C.Get(path)
	if err != nil {
		log.Printf("get %s value err: %s\n", path, err.Error())
	}
	cli.StoreToFile(path, data)

	childrens, _, err := cli.C.Children(path)
	if err != nil {
		log.Printf("get %s children err: %s\n", path, err.Error())
	}
	for _, children := range childrens {
		var newPath string
		if path == "/" {
			newPath = fmt.Sprintf("/%s", children)
		} else {
			newPath = fmt.Sprintf("%s/%s", path, children)
		}
		if len(childrens) > 10 && qps.Num <= maxQps {
			go cli.DumpChildren(newPath)
		} else {
			cli.Dump(newPath)
		}
	}
}

func (cli *ZkCli) DumpChildren(path string) {
	qps.Lock()
	qps.Num += 1
	qps.Unlock()
	defer func() {
		qps.Lock()
		qps.Num -= 1
		qps.Unlock()
	}()
	data, _, err := cli.C.Get(path)
	if err != nil {
		log.Printf("get %s value err: %s\n", path, err.Error())
	}
	cli.StoreToFile(path, data)

	childrens, _, err := cli.C.Children(path)
	if err != nil {
		log.Printf("get %s children err: %s\n", path, err.Error())
	}
	for _, children := range childrens {
		var newPath string
		if path == "/" {
			newPath = fmt.Sprintf("/%s", children)
		} else {
			newPath = fmt.Sprintf("%s/%s", path, children)
		}
		cli.Dump(newPath)
	}
}

func (cli *ZkCli) StoreToFile(path string, data []byte) {
	content := fmt.Sprintf("%s %s\n", path, string(data))
	_, err := f.WriteString(content)
	if err != nil {
		log.Printf("write file err: %s\n", err.Error())
	}
}

func main() {
	log.Print("backup zk data start")
	c := NewZkCli(zkServer, time.Duration(*zkTimeout)*time.Second)
	defer c.Close()
	var err error
	f, err = os.Create(storeFile)
	if err != nil {
		panic("store file err: " + err.Error())
	}
	defer f.Close()
	c.Dump(*zkPath)
	for {
		if qps.Num == 0 {
			break
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
	log.Print("backup zk data done")
}
