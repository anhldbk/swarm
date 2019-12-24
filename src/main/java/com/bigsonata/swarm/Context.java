package com.bigsonata.swarm;

/** @author anhld on 7/28/18 */
public class Context {
  private static Context instance = new Context();
  protected Locust locust = null;
  protected Scheduler scheduler = null;
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

  public Context setScheduler(Scheduler scheduler) {
    this.scheduler = scheduler;
    return this;
  }

  public void recordFailure(Cron cron, long responseTime, String error) {
    this.locust.recordFailure(cron.props.type, cron.props.name, responseTime, error);
    this.scheduler.done();
  }

  public void recordSuccess(Cron cron, long responseTime, long responseLength) {
    this.locust.recordSuccess(cron.props.type, cron.props.name, responseTime, responseLength);
    this.scheduler.done();
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

  public String getNodeId() {
    return this.locust.nodeID;
  }
}
