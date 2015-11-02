#!/bin/bash
cd `dirname $0`
DIR="../../../target/test"
mkdir -p $DIR
cd $DIR

killall nsqd
killall nsqlookupd
sleep 0.1
rm *.dat
nsqlookupd > log-lookup 2>&1 &
sleep 1
nsqd --lookupd-tcp-address=localhost:4160 -broadcast-address=127.0.0.1 -snappy=true -deflate=true > log-nsqd 2>&1 &
echo $! > pid-nsqd
sleep 1

