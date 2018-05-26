package com.bigsonata.swarm.stats;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Created by anhld on 1/28/18. */
public class Histogram {

  public Map<Long, AtomicInteger> map;

  Histogram() {
    map = new ConcurrentHashMap<>();
  }

  void inc(Long k) {
    map.putIfAbsent(k, new AtomicInteger(0));
    map.get(k).incrementAndGet();
  }

  @Override
  public String toString() {
    return this.map.toString();
  }
}
