package service

import (
	"log"

	"github.com/mhseptiadi/blackweb/lib/couchbase"
	"github.com/spf13/viper"
	"gopkg.in/couchbase/gocb.v1"
)

func CouchbaseConn() {
	host := viper.GetString("kafka.host")

	cb, err := gocb.Connect(host)
	if err != nil {
		log.Printf("failed to connect couchbase: %s", err.Error())
	}

	couchbase.InitConn(cb)
}
