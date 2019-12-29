package com.bigsonata.swarm.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Utils {
  private static final Logger logger = LoggerFactory.getLogger(Utils.class);

  public static String md5(String... inputs) {
    try {
      MessageDigest messageDigest = MessageDigest.getInstance("MD5");
      for (String input : inputs) {
        messageDigest.update(input.getBytes());
      }
      byte[] bytes = messageDigest.digest();
      StringBuilder sb = new StringBuilder(33);
      for (int i = 0; i < bytes.length; i++) {
        sb.append(Integer.toHexString((bytes[i] & 0xFF) | 0x100), 1, 3);
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException ex) {
      logger.error(ex.getMessage());
      ex.printStackTrace();
      return null;
    }
  }

  public static String getNodeID(int node) {
    node = node % Snowflake.MAX_NODE;
    return "swarm-" + Snowflake.getInstance(node).nextId();
  }

  public static long round(long value, int places) {
    double round;
    double pow = Math.pow(10, places);
    double digit = pow * value;
    double div = digit % 1;
    if (div > 0.5f) {
      round = Math.ceil(digit);
    } else {
      round = Math.floor(digit);
    }
    double result = round / pow;
    return (long) result;
  }

  /**
   * Get the current timestamp in millis.
   *
   * @return The current timestamp
   */
  public static long now() {
    return System.currentTimeMillis();
  }

  /**
   * Get the current timestamp in seconds.
   *
   * @return The current timestamp
   */
  public static long currentTimeInSeconds() {
    return now() / 1000;
  }
}
