#!/bin/bash

go build

killall pando-bloom

sleep  10
nohup ./pando-bloom 2>&1 &