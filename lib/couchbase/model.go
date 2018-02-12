package couchbase

type Message struct {
	Key  string
	Name string
	Body string
	Time int64
}

type DomainData struct {
	key    string
	name   string
	status string
	date   struct {
		created string
		update  string
		delete  string
	}
	html struct {
		head        string
		body        string
		externalUrl []string
	}
	value struct { //badword occurance compare to total text
		head        int
		body        int
		externalUrl []int
	}
}
