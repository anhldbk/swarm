package com.bigsonata.swarm.benchmark.http;

import com.bigsonata.swarm.Cron;
import com.bigsonata.swarm.Locust;
import com.bigsonata.swarm.benchmark.http.cron.HttpAsyncCron;
import com.bigsonata.swarm.benchmark.http.cron.HttpSyncCron;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Benchmark {
    private static final String LOCUST_MASTER_HOST =
            Registry.ENTRY_LOCUST_HOST.valueString("127.0.0.1");
    private static final int LOCUST_MASTER_PORT = Registry.ENTRY_LOCUST_PORT.valueInt(557);
    private static final int LOCUST_PARALLELISM = Registry.ENTRY_LOCUST_PARALLELISM.valueInt(16);
    private static final int DISRUPTOR_CAPACITY =
            Registry.ENTRY_LOCUST_RING_BUFFER_SIZE.valueInt(2048);
    private static final int EXECUTOR_THREADS = Registry.ENTRY_EXECUTOR_THREADS.valueInt(24);
    private static final String STRING_MODE_ASYNC = "async";
    private static final String CRON_MODE = Registry.ENTRY_CRON_MODE.valueString(STRING_MODE_ASYNC);
    private Locust locust;

    public static Benchmark newInstance() {
        return new Benchmark();
    }

    public void start() {
        List<Cron> crons = getCrons();
        locust.run(crons);
    }

    public void initialize() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(EXECUTOR_THREADS);
        HttpAsyncCron.executor = executorService;

        locust =
                Locust.Builder.newInstance()
                        .setMasterHost(LOCUST_MASTER_HOST)
                        .setMasterPort(LOCUST_MASTER_PORT)

                        // Optionally signal the interval (in ms) to report statistics
                        // Default: 2000
                        .setStatInterval(2000)

                        // Optionally signal a seed number to generate nodeId
                        .setRandomSeed(0)

                        // Optionally signal the capacity for our Disruptor's ring
                        // Note: Must be a power-of-2 number
                        // Default: 1024
                        .setDisruptorCapacity(DISRUPTOR_CAPACITY)

                        // Optionally signal the number of threads used by Disruptor
                        // Default: 4
                        .setDisruptorParallelism(LOCUST_PARALLELISM)
                        .build();
    }

    private List<Cron> getCrons() {
        List<Cron> crons = new ArrayList<>();
        crons.add(
                CRON_MODE.equalsIgnoreCase(STRING_MODE_ASYNC)
                        ? new HttpAsyncCron()
                        : new HttpSyncCron());
        return crons;
    }
}
