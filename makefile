SHELL := /bin/bash

qa-a:
	go run main.go -mode=qa -transfer=account

qa-w:
	go run main.go -mode=qa -transfer=wallet


prod-a:
	go run main.go -mode=prod -transfer=account
	
prod-w:
	go run main.go -mode=prod -transfer=wallet