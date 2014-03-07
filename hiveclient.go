/*
Hive:  The go thrift library for connecting to hive server.

This is just the generated Thrift-Hive and a very small connection wrapper.


Usage:

    func main() {

      hive.MakePool("192.168.1.17:10000")

      conn, err := GetHiveConn()
      if err == nil {
        er, err := conn.Client.Execute("SELECT * FROM logevent")
        if er == nil && err == nil {
          for {
            row, _, _ := conn.Client.FetchOne()
            log.Println("row ", row)
          }
        }
      }
      if conn != nil {
        // make sure to check connection back into pool
        conn.Checkin()
      }
    }

*/
package hive

import (
	//	"crypto/tls"
	"errors"
	"git.apache.org/thrift.git/lib/go/thrift"
	thrifthive "github.com/mattbaird/hive/thriftlib/hive_service"
	"log"
)

type HiveConnection struct {
	Server string
	Id     int
	Client *thrifthive.ThriftHiveClient
}

var hivePool chan *HiveConnection

// create connection pool, initialize connections
func MakePool(server string) {

	hivePool = make(chan *HiveConnection, 100)

	for i := 0; i < 100; i++ {
		// add empty values to the pool
		hivePool <- &HiveConnection{Server: server, Id: i}
	}

}

// main entry point for checking out a connection from a list
func GetHiveConn() (conn *HiveConnection, err error) {
	//configMu.Lock()
	//keyspaceConfig, ok := configMap[keyspace]
	//if !ok {
	//  configMu.Unlock()
	//  return nil, errors.New("Must define keyspaces before you can get connection")
	//}
	//configMu.Unlock()

	return getConnFromPool()
}

func getConnFromPool() (conn *HiveConnection, err error) {

	conn = <-hivePool
	log.Printf("in checkout, pulled off pool: remaining = %d, connid=%d Server=%s\n", len(hivePool), conn.Id, conn.Server)
	// BUG(ar):  an error occured on batch mutate <nil> <nil> <nil> Cannot read. Remote side has closed. Tried to read 4 bytes, but only got 0 bytes.
	if conn.Client == nil || conn.Client.Transport.IsOpen() == false {
		err = conn.Open()
		log.Printf("in create conn, how is client? %v err [%v]", conn.Client, err)
		return conn, err
	}
	return
}

// opens a hive connection
func (conn *HiveConnection) Open() error {

	log.Println("creating new hive connection ")
	var transport thrift.TTransport
	var err error
	transport, err = thrift.NewTSocket(conn.Server)
	if err != nil {
		return err
	}
	if transport == nil {
		return errors.New("No TSocket connection?")
	}

	transport.Open()

	// NewTBinaryProtocolTransport(t TTransport) *TBinaryProtocol {
	protocolfac := thrift.NewTBinaryProtocolFactoryDefault()

	//NewThriftHiveClientProtocol(t thrift.TTransport, iprot thrift.TProtocol, oprot thrift.TProtocol)
	conn.Client = thrifthive.NewThriftHiveClientFactory(transport, protocolfac)

	log.Printf("is open? %v", transport.IsOpen())
	name, err := conn.Client.GetName()
	log.Printf("in conn.Open, how is client? %v %v", name, err)

	if conn.Client == nil {
		log.Println("ERROR, no client")
		return errors.New("no client")
	}

	return nil
}

func (conn *HiveConnection) Checkin() {

	hivePool <- conn
}
