#!/usr/bin/env bash

nohup java -Dlogback.configurationFile=./conf/logback.xml \
    -DappRegistry=./conf/registry.properties \
    -cp ./bin/swarm-http-benchmark-0.1.0-jar-with-dependencies.jar \
    com.bigsonata.swarm.benchmark.http.Runner &