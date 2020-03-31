#!/bin/bash
protoc -I=./\
 --java_out=../src/main/java/\
 --java_out=../../om-server/src/main/java/\
 *.proto

echo "200 OK finished"