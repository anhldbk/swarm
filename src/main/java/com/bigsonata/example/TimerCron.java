package com.bigsonata.example;

import com.bigsonata.swarm.Cron;
import com.bigsonata.swarm.Props;

public class TimerCron extends Cron {
  public TimerCron() {
    super(Props.create().setName("timer").setAsync(false));
  }

  @Override
  public void process() {
    try {
      System.out.println("> Processing...");
      long start = System.currentTimeMillis();
      Thread.sleep(1000);
      long duration = System.currentTimeMillis() - start;
      recordSuccess(duration);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Override
  public Cron clone() {
    return new TimerCron();
  }

  @Override
  public void dispose() {}

  @Override
  public void initialize() {}
}
