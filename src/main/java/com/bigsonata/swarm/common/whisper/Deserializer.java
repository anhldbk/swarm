package com.bigsonata.swarm.common.whisper;

public interface Deserializer<T> {
  T apply(byte[] input);
}
