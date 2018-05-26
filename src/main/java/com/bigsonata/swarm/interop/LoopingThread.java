package com.bigsonata.swarm.interop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class LoopingThread implements Runnable {
  protected int interval = 0;
  private Thread thread = null;
  private Logger logger = null;

  public LoopingThread(String name) {
    this(name, 0);
  }

  /**
   * Construct an auto-start looping thread with delays between consecutive processing steps
   *
   * @param name
   * @param interval
   */
  public LoopingThread(String name, int interval) {
    super();
    String loggerName = LoopingThread.class.getCanonicalName() + "\t[" + name + "]\t";
    logger = LoggerFactory.getLogger(loggerName);
    if (interval >= 0) {
      this.interval = interval;
    }

    start();
  }

  public void initialize() {}

  @Override
  public void run() {
    initialize();

    while (!Thread.currentThread().isInterrupted()) {
      try {
        if (process() == Action.BREAK) {
          //                    thread = null;
          break;
        }
        if (interval >= 0) {
          Thread.sleep(interval);
        }
      } catch (InterruptedException e) {
        break;
      } catch (Exception e) {
        // whatever
      }
    }
    thread = null;
  }

  /**
   * Override this method in your descendant classes Returns false will break the loop NOTE: It will
   * be invoked all over again
   */
  public abstract Action process() throws Exception;

  protected void start() {
    thread = new Thread(this);
    thread.start();
  }

  public void restart() {
    dispose();
    start();
  }

  public void dispose() {
    logger.info("Disposing...");
    if (thread == null) {
      return;
    }
    thread.interrupt();
    try {
      thread.join(2000);
    } catch (InterruptedException e) {
      //            e.printStackTrace();
    }
    thread = null;
  }

  public enum Action {
    BREAK,
    CONTINUE
  }
}
