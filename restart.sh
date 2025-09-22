#!/bin/bash
git pull
go build

killall pando-bloom

sleep  30
nohup ./pando-bloom 2>&1 &