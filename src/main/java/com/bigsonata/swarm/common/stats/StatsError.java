package com.bigsonata.swarm.common.stats;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Created by anhld on 1/28/18. */
public class StatsError {

  protected String name;
  protected String method;
  protected String error;
  protected AtomicLong occurrences = new AtomicLong(0);

  public StatsError(String name, String method, String error) {
    this.name = name;
    this.method = method;
    this.error = error;
  }

  public void occurred() {
    this.occurrences.incrementAndGet();
  }

  public Map<String, Object> toMap() {
    Map<String, Object> m = new HashMap<String, Object>(4);
    m.put("name", this.name);
    m.put("method", this.method);
    m.put("error", this.error);
    m.put("occurrences", this.occurrences.get());
    return m;
  }
}
