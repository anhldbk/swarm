package com.bigsonata.swarm;

public class Props {
  protected String type = "default";
  protected String name = "cron";
  protected int weight = 1;
  protected boolean async = false;

  private Props() {}

  private Props(String type, String name, int weight, boolean async) {
    this.type = type;
    this.name = name;
    this.weight = weight;
    this.async = async;
  }

  public static Props create() {
    return new Props();
  }

  public static Props createAsync() {
    return new Props().setAsync(true);
  }

  public Props setType(String type) {
    this.type = type;
    return this;
  }

  public Props setName(String name) {
    this.name = name;
    return this;
  }

  public Props setWeight(int weight) {
    this.weight = weight;
    return this;
  }

  public Props setAsync(boolean async) {
    this.async = async;
    return this;
  }
}
