package com.bigsonata.swarm.benchmark.http;

public class Runner {
    public static void main(String[] args) throws Exception {
        Registry.initialize();

        Benchmark benchmark = Benchmark.newInstance();
        benchmark.initialize();
        benchmark.start();
    }
}
