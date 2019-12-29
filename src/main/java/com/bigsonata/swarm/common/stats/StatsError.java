package com.bigsonata.swarm.common.stats;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Created by anhld on 1/28/18. */
public class StatsError {

  protected String name;
  protected String method;
  protected String error;
  protected AtomicLong occurences = new AtomicLong(0);

  public StatsError(String name, String method, String error) {
    this.name = name;
    this.method = method;
    this.error = error;
  }

  public void occured() {
    this.occurences.incrementAndGet();
  }

  public Map<String, Object> toMap() {
    Map<String, Object> m = new HashMap<String, Object>(4);
    m.put("name", this.name);
    m.put("method", this.method);
    m.put("error", this.error);
    // there's a typo in Locust rpc protocol
    m.put("occurences", this.occurences.get());
    return m;
  }
}
