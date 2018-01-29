package kafka

import "log"

func Test(massage []byte) {
	log.Printf("Callback function test called to consume %s\n", massage)
}

func Coba(massage []byte) {
	log.Printf("Callback function coba called to consume %s\n", massage)
}
