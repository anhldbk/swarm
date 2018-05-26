#!/usr/bin/env bash

# start locust master
nohup locust -f bin/dummy.py --master --master-bind-host=127.0.0.1 \
    --master-bind-port=5557 \
    --web-port=10001 &