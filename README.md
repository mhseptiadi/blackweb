# BlackWeb

The big idea of this project is:
- bot to crawl blacklisted website (such as porn or torrent) and find external url to be listed as candidate of blacklisted website
- rest api to submit "should be blocked" website
- CMS to manage blacklisted website
- client to be installed in PC or router to block request to blacklisted website (must be distributed freely)
- the client it self can be configure to block only porn, torrent or other type
- sparated database, master database for result from crawl bot, the other is for client in the PC or router
- client database will sync daily to master database

## Requirements

- golang
- couchbase

## Development

- cat requirement.txt | xargs go get
- go run gin.go
