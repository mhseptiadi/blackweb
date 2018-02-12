package service

import "github.com/mhseptiadi/blackweb/lib/couchbase"

func CouchbaseConn() {
	couchbase.InitConn()
	couchbase.Test()
}
