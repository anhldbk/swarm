package com.bigsonata.swarm;

public abstract class Cron implements Cloneable, Runnable {
  protected final Props props;
  private final Context context = Context.getInstance();

  public Cron(Props props) {
    this.props = props;
  }

  public int getWeight() {
    return this.props.weight;
  }

  public String getName() {
    return this.props.name;
  }

  public abstract void process();

  public abstract Cron clone();

  public void recordFailure(long responseTime, String error) {
    this.recordFailure(this.props.type, responseTime, error);
  }

  public void recordFailure(String type, long responseTime, String error) {
    this.context.recordFailure(this, responseTime, error);
  }

  public void recordSuccess(String type, long responseTime, long responseLength) {
    this.context.recordSuccess(this, responseTime, responseLength);
  }

  public void recordSuccess(String type, long responseTime) {
    this.recordSuccess(type, responseTime, 0);
  }

  public void recordSuccess(long responseTime, long responseLength) {
    this.recordSuccess(this.props.type, responseTime, responseLength);
  }

  public void recordSuccess(long responseTime) {
    this.recordSuccess(this.props.type, responseTime, 0);
  }

  @Override
  public void run() {
    if (context.locust.isRunning()) {
      process();
    }
  }

  public abstract void dispose();

  public abstract void initialize();
}
