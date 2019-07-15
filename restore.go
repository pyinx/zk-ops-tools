package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"io"
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
	zkFile    = flag.String("backupFile", "", "backup data file")
)

type Qps struct {
	sync.Mutex
	Num int
}

var (
	storeFile string
	zkServer  []string
	qps       = Qps{Num: 0}
	maxQps    = 100
)

func init() {
	flag.Parse()
	zkServer = []string{fmt.Sprintf("%s:%d", *zkHost, *zkPort)}

	if !strings.HasPrefix(*zkPath, "/") {
		panic("zk path must start with /")
	}
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

func (cli *ZkCli) Restore(key, value string) {
	qps.Lock()
	qps.Num += 1
	qps.Unlock()
	defer func() {
		qps.Lock()
		qps.Num -= 1
		qps.Unlock()
	}()
	data, stat, err := cli.C.Get(key)
	if err != nil {
		if strings.Contains(err.Error(), "node does not exist") {
			_, err := cli.C.Create(key, []byte(value), 0, zk.WorldACL(zk.PermAll))
			if err != nil {
				log.Printf("set %s value err: %s\n", key, err.Error())
			}
		} else {
			log.Printf("get %s value err: %s\n", key, err.Error())
		}
	} else {
		if string(data) != value {
			_, err := cli.C.Set(key, []byte(value), stat.Version)
			if err != nil {
				log.Printf("set %s value err: %s\n", key, err.Error())
			}
		}
	}
}

func (cli *ZkCli) ReadFromeFile(filename string) {
	f, err := os.Open(filename)
	if err != nil {
		log.Printf("open %s err: %s\n", filename, err)
		return
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("read %s err: %s\n", filename, err)
			}
			return
		}
		for {
			if qps.Num <= maxQps {
				break
			} else {
				time.Sleep(10 * time.Millisecond)
			}
		}
		key := strings.Split(line, " ")[0]
		value := strings.Replace(line, key+" ", "", 1)
		go cli.Restore(key, strings.TrimSpace(value))
	}
}

func main() {
	log.Print("restore data to zk from file")
	c := NewZkCli(zkServer, time.Duration(*zkTimeout)*time.Second)
	defer c.Close()
	c.ReadFromeFile(*zkFile)
	for {
		if qps.Num == 0 {
			break
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
	log.Print("restore data to zk done")
}
