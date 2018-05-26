package com.bigsonata.swarm.common.whisper;

public interface Serializer<T> {
  byte[] apply(T input);
}
