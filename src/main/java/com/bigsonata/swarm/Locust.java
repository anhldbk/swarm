package com.bigsonata.swarm;

import com.bigsonata.swarm.common.AtomicCounter;
import com.bigsonata.swarm.common.Utils;
import com.bigsonata.swarm.common.whisper.MessageHandler;
import com.bigsonata.swarm.common.whisper.disruptor.DisruptorBroker;
import com.bigsonata.swarm.interop.Message;
import com.bigsonata.swarm.interop.Transport;
import com.bigsonata.swarm.interop.ZeroTransport;
import com.bigsonata.swarm.stats.RequestFailure;
import com.bigsonata.swarm.stats.RequestSuccess;
import com.bigsonata.swarm.stats.StatsService;
import com.google.common.base.Joiner;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Locust class exposes all the APIs of locust4j. Use Locust.getInstance() to get a Locust
 * singleton.
 */
public class Locust {
  private static final Logger logger = LoggerFactory.getLogger(Locust.class.getCanonicalName());
  private static Locust instance = null;
/**
   * Every locust4j instance registers a unique nodeID to the master when it makes a connection.
   * NodeID is kept by Runner.
   */
  protected String nodeID = null;
  /** Number of clients required by the master, locust4j use threads to simulate clients. */
  protected int numClients = 0;
  Context context = null;
  private Builder builder;
  private Transport transport = null;
  private boolean started = false;
  private Processor cronProcessor;
  private StatsService statsService = null;
  /** Current state of runner. */
  private AtomicReference<State> state = new AtomicReference<>(State.IDLE);
  /** Task instances submitted by user. */
  private List<Cron> prototypes;
  private List<Cron> crons = new ArrayList<>();
  /** Hatch rate required by the master. Hatch rate means clients/s. */
  private int hatchRate = 0;

  private Locust(Builder builder) {
    this.builder = builder;
    this.nodeID = Utils.getNodeID(builder.randomSeed);
    initialize();

    Locust.instance = this;
  }

  private void initialize() {
    initializeCronProcessor();
    initializeShutdownHook();
    initializeContext();
    initializeLocustClient();
    initializeStatsService();
  }

  private void initializeContext() {
    this.context =
        Context.getInstance()
            .setLocust(this)
            .setMasterHost(builder.masterHost)
            .setMasterPort(builder.masterPort)
            .setStatInterval(builder.statInterval)
            .setMaxPending(builder.disruptorCapacity)
            .setProcessor(cronProcessor);
  }

  private void initializeCronProcessor() {
    cronProcessor = new Processor(builder.disruptorParallelism, builder.disruptorCapacity);
  }

  private void initializeLocustClient() {
    transport =
        new ZeroTransport(context) {
          @Override
          public void onMessage(Message message) throws Exception {
            Locust.this.onMessage(message);
          }

          @Override
          public void onConnected() {
            super.onConnected();
            sendReady();
          }
        };

    try {
      transport.initialize();
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("Failed to initialize Locust transport. Terminating now...");
      System.exit(-1);
    }
  }

  private void initializeStatsService() {
    statsService =
        new StatsService(context) {
          @Override
          public void onData(Map data) {
            Locust.this.sendReport(data);
          }
        };
    statsService.initialize();
  }

  private void sendReport(Map data) {
    State currentState = this.state.get();

    boolean updatable = (currentState == State.Running || currentState == State.Hatching);
    if (!updatable) {
      // no need to send report
      return;
    }

    data.put("user_count", numClients);
    String report = Joiner.on(", ").withKeyValueSeparator(" -> ").join(data);

    try {
      transport.send(new Message("stats", data, nodeID));
    } catch (Exception ex) {
      ex.printStackTrace();
      logger.error(ex.getMessage());
    }
  }

  private void onMessage(Message message) throws Exception {
    String type = message.getType();
    if (message.isHatch()) {
      transport.send(new Message("hatching", null, nodeID));
      Float hatchRate = Float.valueOf(message.getData().get("hatch_rate").toString());
      int numClients = Integer.valueOf(message.getData().get("num_clients").toString());
      startHatching(numClients, hatchRate.intValue());
      return;
    }

    if (message.isStop()) {
      stop();
      transport.send(new Message("client_stopped", null, nodeID));
      transport.send(new Message("client_ready", null, nodeID));
      return;
    }

    if (message.isQuit()) {
      logger.info("Got `quit` message from master, shutting down...");
      System.exit(0);
    }
  }

  /**
   * Add prototypes to Runner, connect to master and wait for messages of master.
   *
   * @param crons
   */
  public void run(Cron... crons) {
    List<Cron> crs = new ArrayList<>();
    for (Cron task : crons) {
      crs.add(task);
    }
    run(crs);
  }

    /**
   * Add prototypes to Runner, connect to master and wait for messages of master.
   *
   * @param crons
   */
  public synchronized void run(List<Cron> crons) {
    if (this.started) {
      // Don't call Locust.run() multiply times.
      return;
    }

    this.prototypes = crons;
    this.state.set(State.Ready);
    this.started = true;
  };

  /**
   * when JVM is shutting down, send a `quit` message to master, then master will remove this slave
   * from its list.
   */
  private void initializeShutdownHook() {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  logger.warn("JVM is shutting down...");
                  // tell master that I'm quitting
                  dispose();
                }));
  }

  /**
   * Add a successful record, locust4j will collect it, calculate things like RPS, and sendReport to
   * master.
   *
   * @param type
   * @param name
   * @param responseTime
   * @param contentLength
   */
  public void recordSuccess(String type, String name, long responseTime, long contentLength) {
    RequestSuccess success = new RequestSuccess();
    success.type = type;
    success.name = name;
    success.responseTime = responseTime;
    success.contentLength = contentLength;
    statsService.report(success);
  }

  public void recordSuccess(String type, String name, long responseTime) {
    this.recordSuccess(type, name, responseTime, 0);
  }

  /**
   * Add a failed record, locust4j will collect it, and sendReport to master.
   *
   * @param type
   * @param name
   * @param responseTime
   * @param error
   */
  public void recordFailure(String type, String name, long responseTime, String error) {
    RequestFailure failure = new RequestFailure();
    failure.type = type;
    failure.name = name;
    failure.responseTime = responseTime;
    failure.error = error;
    statsService.report(failure);
  }

  protected State getState() {
    return this.state.get();
  }

  public boolean isStopped() {
    return getState().equals(Locust.State.Stopped);
  }

  private void spawnWorkers(int spawnCount) {
    logger.info(
        "Hatching and swarming {} clients at the rate of {} clients/s...",
        spawnCount,
        this.hatchRate);
    final RateLimiter rateLimiter = RateLimiter.create(this.hatchRate);

    float weightSum = 0;
    for (Cron cron : this.prototypes) {
      weightSum += cron.getWeight();
    }

    // rescale
    for (Cron prototype : this.prototypes) {
      float percent;

      if (0 == weightSum) {
        percent = 1 / (float) this.prototypes.size();
      } else {
        percent = prototype.getWeight() / weightSum;
      }

      int amount = Math.round(spawnCount * percent);
      if (weightSum == 0) {
        amount = spawnCount / this.prototypes.size();
      }

      logger.info("> {}={}", prototype.getName(), amount);

      for (int i = 1; i <= amount; i++) {
        rateLimiter.acquire();
        Cron clone = prototype.clone();
        clone.initialize(); // initialize them first
        this.crons.add(clone);
      }
    }

    for (Cron cron : this.crons) {
      this.context.schedule(cron);
    }

    this.onHatchCompleted();
  }

  protected void startHatching(int spawnCount, int hatchRate) {
    State currentState = this.state.get();
    if (currentState != State.Running && currentState != State.Hatching) {
      statsService.clearAll();
      this.numClients = spawnCount;
    }
    if (currentState == State.Running) {
      this.dispose();
    }
    logger.info("Start hatching...");
    logger.info("> spawnCount={}", spawnCount);
    logger.info("> hatchRate={}", hatchRate);

    this.state.set(State.Hatching);
    this.hatchRate = hatchRate;
    this.numClients = spawnCount;
    this.spawnWorkers(numClients);
  }

  protected void onHatchCompleted() {
    Map data = new HashMap(1);
    data.put("count", this.numClients);
    try {
      transport.send(new Message("hatch_complete", data, this.nodeID));
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("Can NOT send `hatch_complete` message");
    }
    this.state.set(State.Running);
  }

  protected void sendQuit() {
    logger.info("Quitting...");
    try {
      transport.send(new Message("quit", null, this.nodeID));
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("Can NOT send `quit` message");
    }
  }

  protected void sendReady() {
    logger.info("Ready!");
    try {
      transport.send(new Message("client_ready", null, this.nodeID));
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("Can NOT send `client_ready` message");
    }
  }

  private void dispose() {
    logger.warn("Disposing...");
    sendQuit();

    this.state.set(State.Stopped);
    transport.dispose();

    for (Cron cron : prototypes) {
      cron.dispose();
    }
    cronProcessor.dispose();

    logger.info("Bye bye!");
  }

  protected void stop() {
    if (this.state.get() != State.Running) {
      return;
    }
    logger.info("Received message STOP from master, all the workers are stopped");
    this.state.set(State.Stopped);

    for (Cron cron : this.crons) {
      cron.dispose();
    }
    this.crons.clear();
  }

  /** State of runner */
  enum State {
    IDLE,
    /** Runner is ready to receive message from master. */
    Ready,

    /** Runner is submitting prototypes to its thread pool. */
    Hatching,

    /** Runner is done with submitting prototypes. */
    Running,

    /** Runner is stopped, its thread pool is destroyed, the test is stopped. */
    Stopped,
  }

  public static class Builder {
    private String masterHost = "127.0.0.1";
    private int masterPort = 5557;
    private int disruptorCapacity = 1024;
    private int disruptorParallelism = 8;
    private int statInterval = 2000;
    private int randomSeed = 0;

    public static Builder newInstance() {
      return new Builder();
    }

    public Locust build() throws Exception {
      if ((disruptorCapacity & (disruptorCapacity - 1)) != 0) {
        throw new Exception("Disruptor capacity must be a power of 2");
      }
      return new Locust(this);
    }

    /**
     * Set master host.
     *
     * @param masterHost
     */
    public Builder setMasterHost(String masterHost) {
      this.masterHost = masterHost;
      return this;
    }

    /**
     * Set master port.
     *
     * @param masterPort
     */
    public Builder setMasterPort(int masterPort) {
      this.masterPort = masterPort;
      return this;
    }

    public Builder setDisruptorCapacity(int disruptorCapacity) {
      this.disruptorCapacity = disruptorCapacity;
      return this;
    }

    public Builder setDisruptorParallelism(int disruptorParallelism) {
      this.disruptorParallelism = disruptorParallelism;
      return this;
    }

    public Builder setStatInterval(int statInterval) {
      this.statInterval = statInterval;
      return this;
    }

    public Builder setRandomSeed(int randomSeed) {
      this.randomSeed = randomSeed;
      return this;
    }
  }

  public static class Processor {
    private static final Logger logger = LoggerFactory.getLogger(Processor.class);
    private RateLimiter rateLimiter = null;
    private ExecutorService executor = null;
    private DisruptorBroker<Runnable> disruptor = null;

    public Processor(int parallelism, int capacity) {
      this(parallelism, capacity, -1);
    }

    public Processor(int parallelism, int capacity, int requestRate) {
      initialize(parallelism, capacity, requestRate);
    }

    public Processor() {
      this(1, 1024);
    }

    public Processor(int parallelism) {
      this(parallelism, 1024);
    }

    private void initialize(int parallelism, int capacity, int requestRate) {
      MessageHandler<Runnable> handler =
          (s, task) -> {
            try {
              task.run();
            } catch (Exception e) {
              e.printStackTrace();
              logger.error("Error when processing events. Detail: {}", e.getMessage());
            }
          };
      try {
        executor = Executors.newFixedThreadPool(1);
        disruptor =
            DisruptorBroker.<Runnable>newBuilder()
                .setBufferSize(capacity)
                .setMessageHandler(handler)
                .setParallelism(parallelism)
                .build();
        disruptor.initialize();
      } catch (Exception e) {
        e.printStackTrace();
        logger.error("Can NOT initialize. Terminating now...");
        System.exit(-1);
      }

      if (requestRate > 0) {
        rateLimiter = RateLimiter.create(requestRate);
      }
    }

    public void dispose() {
      if (disruptor == null) {
        return;
      }
      logger.warn("Disposing ...");
      executor.shutdownNow();
      try {
        disruptor.dispose();
      } catch (Exception e) {
        // don't care
      }
    }

    public void submit(Runnable task) {
      if (rateLimiter != null) {
        rateLimiter.acquire();
      }
      try {
        disruptor.produce(null, task);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    public void submitAsync(Runnable task) {
      executor.submit(() -> submit(task));
    }
  }

  public static class Context {
    private static Context instance = new Context();
    protected Locust locust = null;
    protected Processor processor = null;
    protected int maxPending = 0;
    protected AtomicCounter pending = new AtomicCounter(0);
    private int statInterval = 3000; // 3s
    private String masterHost = "127.0.0.1";
    private int masterPort = 7778;

    public static Context getInstance() {
      return instance;
    }

    public Context setLocust(Locust locust) {
      this.locust = locust;
      return this;
    }

    public Context setProcessor(Processor processor) {
      this.processor = processor;
      return this;
    }

    public Context setMaxPending(int maxPending) {
      if (maxPending > 0) this.maxPending = maxPending;
      return this;
    }

    private void await(Cron cron) {
      if (cron.props.async) this.pending.incIfLessThan(maxPending);
    }

    /**
     * Invoke this method in async mode to notify a processing task is handled If you don't call
     * this method, no more task will be performed
     */
    private void done(Cron cron) {
      // Notify a processing task is handled
      // We must do this to not over-schedule crons
      if (cron.props.async) {
        this.pending.decIfPositive();
      }
    }

    public void recordFailure(Cron cron, long responseTime, String error) {
      this.locust.recordFailure(cron.props.type, cron.props.name, responseTime, error);
      done(cron);
    }

    public void recordSuccess(Cron cron, long responseTime, long contentLength) {
      this.locust.recordSuccess(cron.props.type, cron.props.name, responseTime, contentLength);
      done(cron);
    }

    /**
     * Invoke this method to schedule a cron to run in the future
     *
     * @param cron A cron
     */
    public void schedule(Cron cron) {
      if (this.locust.isStopped()) {
        return;
      }
      this.await(cron);
      this.processor.submitAsync(cron);
    }

    public int getStatInterval() {
      return statInterval;
    }

    public Context setStatInterval(int statInterval) {
      this.statInterval = statInterval;
      return this;
    }

    public int getMasterPort() {
      return masterPort;
    }

    public Context setMasterPort(int masterPort) {
      this.masterPort = masterPort;
      return this;
    }

    public String getMasterHost() {
      return masterHost;
    }

    public Context setMasterHost(String masterHost) {
      this.masterHost = masterHost;
      return this;
    }
  }
}
