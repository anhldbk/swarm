package com.bigsonata.example;

import com.bigsonata.swarm.Locust;

public class Benchmark implements Runnable {
  private static final String LOCUST_MASTER_HOST = "127.0.0.1";
  private static final int LOCUST_MASTER_PORT = 5557;
  private static final int LOCUST_PARALLELISM = 8;

  public static void main(String[] args) {
    new Benchmark().run();
  }

  @Override
  public void run() {
    try {
      Locust.Builder.newInstance()
          .setMasterHost(LOCUST_MASTER_HOST)
          .setMasterPort(LOCUST_MASTER_PORT)

          // Optionally signal the interval (in ms) to report statistics
          // Default: 2000
          .setStatInterval(2000)

          // Optionally signal a seed number to generate nodeId
          .setRandomSeed(0)

          // Optionally signal the number of threads used by Disruptor
          // Default: 4
          .setThreads(LOCUST_PARALLELISM)

          // Set crons to be scheduled
          .setCrons(new TimerCron())
          .build();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
