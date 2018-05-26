package com.bigsonata.swarm.common.whisper;

public class Result {
  public boolean successful = false;
  public String message = "";

  private Result() {}

  public static Result success() {
    Result result = new Result();
    result.successful = true;
    return result;
  }

  public static Result error(String message) {
    Result result = new Result();
    result.successful = false;
    result.message = message;
    return result;
  }

  public static Result error() {
    Result result = new Result();
    result.successful = false;
    return result;
  }

  public String toString() {
    return String.format("success=%s\tmessage=%s", successful, message);
  }
}
