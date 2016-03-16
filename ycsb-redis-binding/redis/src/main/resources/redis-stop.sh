#!/bin/bash

echo "Stopping all redis servers on ports 6001, 6002, 6003..."
ps aux | grep redis-server | awk '{print $2}' | xargs kill
