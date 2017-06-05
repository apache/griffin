#!/bin/bash

curl -X POST -H "Content-Type: application/json" -d '{
	"name": "viewitem_hourly",
	"description": "hourly accuracy of viewitem data",
	"type": "accuracy",
	"source": {},
	"target": {},
	"evaluateRule": {},
	"owner": "test",
	"organization": "demo"
}' "localhost:8080/measures/add/"

curl -X POST -H "Content-Type: application/json" -d '{
	"name": "search_hourly",
	"description": "hourly accuracy of viewitem data",
	"type": "accuracy",
	"source": {},
	"target": {},
	"evaluateRule": {},
	"owner": "test",
	"organization": "demo"
}' "localhost:8080/measures/add/"

curl -X POST -H "Content-Type: application/json" -d '{
	"name": "buy_hourly",
	"description": "hourly accuracy of viewitem data",
	"type": "accuracy",
	"source": {},
	"target": {},
	"evaluateRule": {},
	"owner": "test",
	"organization": "demo"
}' "localhost:8080/measures/add/"
