#!/bin/bash

echo "Starting redis server 1 on port 6001..."
redis-server --port 6001 &
sleep 1
echo "Started redis server 1"

echo "Starting redis server 2 on port 6002..."
redis-server --port 6002 &
sleep 1
echo "Started redis server 2"

echo "Starting redis server 3 on port 6003..."
redis-server --port 6003 &
sleep 1
echo "Started redis server 3"
