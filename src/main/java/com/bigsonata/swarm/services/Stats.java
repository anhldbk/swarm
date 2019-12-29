package com.bigsonata.swarm.services;

import com.bigsonata.swarm.Context;
import com.bigsonata.swarm.common.Disposable;
import com.bigsonata.swarm.common.Initializable;
import com.bigsonata.swarm.common.Utils;
import com.bigsonata.swarm.interop.LoopingThread;
import com.bigsonata.swarm.common.stats.RequestFailure;
import com.bigsonata.swarm.common.stats.RequestSuccess;
import com.bigsonata.swarm.common.stats.StatsEntry;
import com.bigsonata.swarm.common.stats.StatsError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Stats implements Disposable, Initializable {
  private static final Logger logger =
      LoggerFactory.getLogger(Stats.class.getCanonicalName());
  private LoopingThread statsTimer;
  private Map<String, StatsEntry> entries;
  private Map<String, StatsError> errors;
  private StatsEntry total;
  private int statInterval = 3000;

  public Stats(Context ctx) {
    this.entries = new HashMap<>(8);
    this.errors = new HashMap<>(8);
    this.total = new StatsEntry("Total");
    this.total.reset();
    this.statInterval = ctx.getStatInterval();
  }

  public synchronized void initialize() {
    if (this.statsTimer != null) {
      return;
    }
    logger.info("Initializing...");
    this.statsTimer =
        new LoopingThread("locust-stats", statInterval) {
          @Override
          public Action process() {
            Map data = collectReportData();
            onData(data);
            return Action.CONTINUE;
          }
        };
    logger.info("Initialized");
  }

  /**
   * Override this method to submit stats data
   *
   * @param data Data
   */
  public abstract void onData(Map data);

  public void dispose() {
    this.statsTimer.dispose();
  }

  public void report(RequestSuccess request) {
    logRequest(request.type, request.name, request.responseTime, request.responseLength);
  }

  public void report(RequestFailure request) {
    logError(request.type, request.name, request.error);
  }

  protected StatsEntry get(String name, String method) {
    StatsEntry entry = this.entries.get(name + method);
    if (null == entry) {
      entry = new StatsEntry(name, method);
      entry.reset();
      this.entries.put(name + method, entry);
    }
    return entry;
  }

  protected void logRequest(String method, String name, long responseTime, long responseLength) {
    this.total.log(responseTime, responseLength);
    this.get(name, method).log(responseTime, responseLength);
  }

  protected void logError(String method, String name, String error) {
    this.total.logError(error);
    this.get(name, method).logError(error);

    String key = Utils.md5(method + name + error);
    if (null == key) {
      key = method + name + error;
    }
    StatsError entry = this.errors.get(key);
    if (null == entry) {
      entry = new StatsError(name, method, error);
      this.errors.put(key, entry);
    }
    entry.occured();
  }

  public void clearAll() {
    total = new StatsEntry("Total");
    total.reset();
    entries = new HashMap<>(8);
    errors = new HashMap<>(8);
  }

  protected List serializeStats() {
    List entries = new ArrayList(this.entries.size());
    for (Map.Entry<String, StatsEntry> item : this.entries.entrySet()) {
      StatsEntry entry = item.getValue();
      if (!(entry.numRequests.get() == 0 && entry.numFailures.get() == 0)) {
        entries.add(entry.getStrippedReport());
      }
    }
    return entries;
  }

  protected Map<String, Map<String, Object>> serializeErrors() {
    Map<String, Map<String, Object>> errors = new HashMap(8);
    for (Map.Entry<String, StatsError> item : this.errors.entrySet()) {
      String key = item.getKey();
      StatsError error = item.getValue();
      errors.put(key, error.toMap());
    }
    return errors;
  }

  protected Map<String, Object> collectReportData() {
    Map<String, Object> data = new HashMap<String, Object>(3);

    data.put("stats", this.serializeStats());
    data.put("stats_total", this.total.getStrippedReport());
    data.put("errors", this.serializeErrors());

    return data;
  }
}
