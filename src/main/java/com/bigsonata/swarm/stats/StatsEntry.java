package com.bigsonata.swarm.stats;

import com.bigsonata.swarm.common.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Created by anhld on 1/28/18. */
public class StatsEntry {

  public String name = "";
  public String method = "";
  public AtomicLong numRequests;
  public AtomicLong numFailures;
  public AtomicLong totalResponseTime;
  public AtomicLong minResponseTime;
  public AtomicLong maxResponseTime;
  public Histogram numReqsPerSec;
  public Histogram numFailPerSec;
  public Histogram responseTimes;
  public AtomicLong totalResponseLength;
  public AtomicLong startTime;
  public AtomicLong lastRequestTimestamp;

  public StatsEntry(String name) {
    this.name = name;
  }

  public StatsEntry(String name, String method) {
    this.name = name;
    this.method = method;
  }

  public void reset() {
    this.startTime = new AtomicLong(Utils.currentTimeInSeconds());
    this.numRequests = new AtomicLong(0);
    this.numFailures = new AtomicLong(0);
    this.totalResponseTime = new AtomicLong(0);
    this.responseTimes = new Histogram();
    this.minResponseTime = new AtomicLong(0);
    this.maxResponseTime = new AtomicLong(0);
    this.lastRequestTimestamp = new AtomicLong(Utils.currentTimeInSeconds());
    this.numReqsPerSec = new Histogram();
    this.numFailPerSec = new Histogram();
    this.totalResponseLength = new AtomicLong(0);
  }

  public void log(long responseTime, long responseLength) {
    this.numRequests.incrementAndGet();
    this.logTimeOfRequest();
    this.logResponseTime(responseTime);
    this.totalResponseLength.addAndGet(responseLength);
  }

  public void logTimeOfRequest() {
    long now = Utils.currentTimeInSeconds();
    this.numReqsPerSec.inc(now);
    this.lastRequestTimestamp.set(now);
  }

  public void logResponseTime(long responseTime) {
    this.totalResponseTime.addAndGet(responseTime);

    if (this.minResponseTime.get() == 0) {
      this.minResponseTime.set(responseTime);
    }

    if (responseTime < this.minResponseTime.get()) {
      this.minResponseTime.set(responseTime);
    }

    if (responseTime > this.maxResponseTime.get()) {
      this.maxResponseTime.set(responseTime);
    }

    long roundedResponseTime = 0L;

    if (responseTime < 100) {
      roundedResponseTime = responseTime;
    } else if (responseTime < 1000) {
      roundedResponseTime = Utils.round(responseTime, -1);
    } else if (responseTime < 10000) {
      roundedResponseTime = Utils.round(responseTime, -2);
    } else {
      roundedResponseTime = Utils.round(responseTime, -3);
    }

    this.responseTimes.inc(roundedResponseTime);
  }

  public void logError(String error) {
    long now = Utils.currentTimeInSeconds();
    this.numFailPerSec.inc(now);
    this.numFailures.incrementAndGet();
  }

  public Map<String, Object> serialize() {
    Map<String, Object> result = new HashMap<String, Object>(12);
    result.put("name", this.name);
    result.put("method", this.method);
    result.put("last_request_timestamp", this.lastRequestTimestamp.get());
    result.put("start_time", this.startTime.get());
    result.put("num_requests", this.numRequests.get());
    // Locust4j doesn't allow None response time for requests like locust.
    // num_none_requests is added to keep compatible with locust.
    result.put("num_none_requests", 0);
    result.put("num_failures", this.numFailures.get());
    result.put("total_response_time", this.totalResponseTime.get());
    result.put("max_response_time", this.maxResponseTime.get());
    result.put("min_response_time", this.minResponseTime.get());
    result.put("total_content_length", this.totalResponseLength.get());
    result.put("response_times", this.responseTimes);
    result.put("num_reqs_per_sec", this.numReqsPerSec);
    result.put("num_fail_per_sec", this.numFailPerSec);
    return result;
  }

  public Map<String, Object> getStrippedReport() {
    Map<String, Object> report = this.serialize();
    this.reset();
    return report;
  }
}
