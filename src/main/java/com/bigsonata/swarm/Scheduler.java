package com.bigsonata.swarm;

import com.bigsonata.swarm.common.CircularList;
import com.bigsonata.swarm.common.ConcurrentCircularList;
import com.bigsonata.swarm.common.Disposable;
import com.bigsonata.swarm.common.Initializable;
import com.bigsonata.swarm.common.whisper.DisruptorBroker;
import com.bigsonata.swarm.common.whisper.MessageHandler;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicInteger;

/** @author anhld on 7/28/18 */
public class Scheduler implements Runnable, Disposable, Initializable {
  private static final Logger logger = LoggerFactory.getLogger(Scheduler.class);
  private final Builder builder;
  @Nullable private RateLimiter rateLimiter;
  private Thread executor = null;
  private DisruptorBroker<Runnable> disruptor = null;
  private CircularList<Cron> queue = new ConcurrentCircularList<>();
  protected AtomicInteger pending = new AtomicInteger(0);

  public static class Builder {
    private int parallelism = 8;
    private int bufferSize = 1024;
    private int maxRps = -1;
    private Locust locust;

    public Builder setParallelism(int parallelism) {
      this.parallelism = parallelism;
      return this;
    }

    public Builder setBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    public Builder setMaxRps(int maxRps) {
      this.maxRps = maxRps;
      return this;
    }

    public Builder setLocust(Locust locust) {
      this.locust = locust;
      return this;
    }

    public Scheduler build() {
      return new Scheduler(this);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private Scheduler(Builder builder) {
    this.builder = builder;
    initialize();
  }

  public void initialize() {
    if (builder.maxRps > 0) {
      logger.info("Setting max RPS to {}", builder.maxRps);
      rateLimiter = RateLimiter.create(builder.maxRps);
    }
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
      disruptor =
          DisruptorBroker.<Runnable>newBuilder()
              .setBufferSize(builder.bufferSize)
              .setMessageHandler(handler)
              .setParallelism(builder.parallelism)
              .build();
      disruptor.initialize();

      executor = new Thread(this, "swarm-scheduler");
      executor.start();

    } catch (Exception e) {
      e.printStackTrace();
      logger.error("Can NOT initialize. Terminating now...");
      System.exit(-1);
    }
  }

  /** Invoke this method externally to signify a task is completed */
  public void done() {
    if (this.builder.locust.isStopped()) {
      return;
    }
    this.pending.decrementAndGet();
  }

  /** Wait until our number of pending tasks is not too large */
  private void await() {
    if (rateLimiter != null) {
      rateLimiter.acquire();
    }
    while (this.pending.get() > builder.bufferSize) {
      idle();
    }
  }

  public void stop() {
    queue.forEach((cron) -> cron.dispose());
    queue.clear();
    this.pending.set(0);
  }

  public void dispose() {
    if (disruptor == null) {
      return;
    }
    logger.warn("Disposing ...");
    executor.interrupt();

    stop();

    try {
      disruptor.dispose();
    } catch (Exception e) {
      // don't care
    }
  }

  private void idle() {
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
    }
  }

  public void submit(Cron cron) {
    queue.add(cron);
  }

  @Override
  public void run() {
    while (!Thread.currentThread().isInterrupted()) {
      if (queue.size() == 0 || this.builder.locust.isStopped()) {
        // try not to bombard our CPU
        idle();
        continue;
      }
      queue.forEach(
          (cron) -> {
            await();
            if (this.builder.locust.isStopped()) {
              return;
            }
            this.pending.incrementAndGet();
            try {
              disruptor.produce(cron);
            } catch (Exception e) {
              e.printStackTrace();
            }
          });
    }

    logger.info("Terminating...");
  }
}
