package com.bigsonata.swarm.benchmark.http;

import com.bigsonata.commons.RegistryBase;

public class Registry extends RegistryBase {
    public static final Entry ENTRY_LOCUST_HOST = Entry.create("swarm.benchmark.locust.host");
    public static final Entry ENTRY_LOCUST_PORT = Entry.create("swarm.benchmark.locust.port");
    public static final Entry ENTRY_URL = Entry.create("swarm.benchmark.url");
    public static final Entry ENTRY_LOCUST_PARALLELISM =
            Entry.create("swarm.benchmark.locust.parallelism");
    public static final Entry ENTRY_EXECUTOR_THREADS =
            Entry.create("swarm.benchmark.executor.threads");
    public static final Entry ENTRY_LOCUST_RING_BUFFER_SIZE =
            Entry.create("swarm.benchmark.locust.ring.buffer.size");
    public static final Entry ENTRY_CRON_IO_THREADS =
            Entry.create("swarm.benchmark.cron.io.threads");
    public static final Entry ENTRY_CRON_MODE = Entry.create("swarm.benchmark.cron.mode");
}
