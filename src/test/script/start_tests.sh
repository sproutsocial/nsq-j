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

NSQD_ARGS=$*

nsqd $NSQD_ARGS -lookupd-tcp-address=localhost:4160 -broadcast-address=127.0.0.1 -snappy=true -deflate=true -tls-cert=../../src/test/resources/cert.pem -tls-key=../../src/test/resources/key.pem > log-nsqd 2>&1 &

#for testing auth, difficult to automate
#nsqd -lookupd-tcp-address=localhost:4160 -broadcast-address=127.0.0.1 -auth-http-address=127.0.0.1:4181 -snappy=true -deflate=true -tls-cert=../../src/test/resources/cert.pem -tls-key=../../src/test/resources/key.pem > log-nsqd 2>&1 &

echo $! > pid-nsqd
sleep 1

