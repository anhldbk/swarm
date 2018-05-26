Http Benchmark
================
*A simple demo of using Swarm*

This demo stimulates a load test on a specific configurable URL (method GET). [async-http-client](https://github.com/AsyncHttpClient/async-http-client) is used to make Http requests in `sync` and `async` modes

### 1. Build

```
$ mvn package
```

Pre-built binaries may be found in directory `/dist/bin`

### 2. Configure

The configurations are stored in `dist/conf/registry.properties`

```properties
swarm.benchmark.locust.host=127.0.0.1
swarm.benchmark.locust.port=5557
# Configure your URL here
swarm.benchmark.url=https://bigsonata.com
swarm.benchmark.executor.threads=24
# Crons operate in 2 modes: `sync`, `async`
# Default: ASYNC
swarm.benchmark.cron.mode=async
swarm.benchmark.cron.io.threads=48
swarm.benchmark.locust.parallelism=48
swarm.benchmark.locust.ring.buffer.size=512
```

### 3. Run

Start `Locust` server first (in local mode):

```bash
$ ./start-locust.sh
```

Then activate your benchmarking program:

```bash
$ ./start-benchmark.sh
```

Finally, visit Locust web interface via [http://localhost:10001](http://localhost:10001)