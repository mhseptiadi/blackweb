package couchbase

import (
	"log"

	"github.com/spf13/viper"
	"gopkg.in/couchbase/gocb.v1"
)

var Cluster gocb.Cluster
var ClusterManager *gocb.ClusterManager

type BucketObj struct {
	domain  *gocb.Bucket
	badword *gocb.Bucket
	// ip      *gocb.Bucket
}

var Bucket BucketObj

// var BucketDomain *gocb.Bucket

// var bucketList []*gocb.BucketSettings

func InitConn() *gocb.Cluster {

	host := viper.GetString("couchbase.host")

	Cluster, err := gocb.Connect(host)
	if err != nil {
		log.Fatalf("Couchbase failed to connect: %s", err.Error())
	}

	err = Cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: viper.GetString("couchbase.auth.user"),
		Password: viper.GetString("couchbase.auth.pass"),
	})
	if err != nil {
		log.Fatalf("Couchbase failed to authenticate: %s", err.Error())
	}

	Bucket.domain, err = Cluster.OpenBucket("domain", "")
	if err != nil {
		log.Fatalf("Couchbase failed to open bucket domain: %s", err.Error())
	}

	Bucket.badword, err = Cluster.OpenBucket("badword", "")
	if err != nil {
		log.Fatalf("Couchbase failed to open bucket badword: %s", err.Error())
	}

	// Bucket.ip, err = Cluster.OpenBucket("ip", "")
	// if err != nil {
	// 	log.Fatalf("Couchbase failed to open bucket ip: %s", err.Error())
	// }

	log.Printf("couchbase cluster init")
	return Cluster
}

func Test() (err error) {

	// log.Printf("Bucket : %v", Bucket.domain)
	// log.Printf("Bucket : %v", Bucket.badword)

	m := Message{"1", "Alice", "Hello", 1294706395881547000}
	u := Message{"1", "Alice update", "Hello update", 1294706395881547000}
	Insert(Bucket.domain, m)
	Update(Bucket.domain, u)
	return err
}

func Insert(target *gocb.Bucket, value Message) (err error) {
	_, err = target.Insert(value.Key, value, 0)
	if err != nil {
		log.Printf("Error inserting doc: %s", err.Error())
	}
	return err
}

func Update(target *gocb.Bucket, value Message) (err error) {
	_, err = target.Replace(value.Key, value, 0, 0)
	if err != nil {
		log.Printf("Error inserting doc: %s", err.Error())
	}
	return err
}
