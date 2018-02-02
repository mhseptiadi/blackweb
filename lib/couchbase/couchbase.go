package couchbase

import (
	"log"

	"gopkg.in/couchbase/gocb.v1"
)

var couchbaseCluster gocb.Cluster

func InitConn(cluster *gocb.Cluster) {
	couchbaseCluster = *cluster
	log.Print("couchbase cluster init")
}
