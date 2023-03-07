package com.bigsonata.swarm.services;

import com.bigsonata.swarm.Locust;
import com.bigsonata.swarm.common.Disposable;
import com.bigsonata.swarm.common.Initializable;
import com.bigsonata.swarm.interop.LoopingThread;
import com.bigsonata.swarm.interop.Message;
import com.bigsonata.swarm.interop.Transport;
import com.sun.management.OperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

public class Beat implements Disposable, Initializable {

  private static final Logger logger =
      LoggerFactory.getLogger(Beat.class.getCanonicalName());
  private final OperatingSystemMXBean osBean =
          ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
  private final Transport transport;
  private final Locust locust;

  private LoopingThread heartBeatTimer;

  public Beat(Transport transport, Locust locust) {
    this.transport = transport;
    this.locust = locust;
  }

  @Override
  public void dispose() throws Exception {
    if (null != this.heartBeatTimer) {
      this.heartBeatTimer.dispose();
    }
  }

  @Override
  public void initialize() {
    if (this.heartBeatTimer != null) {
      return;
    }
    logger.info("Initializing...");
    this.heartBeatTimer =
        new LoopingThread("heartbeat-service", 1000) {
          int counter = 0;

          @Override
          public Action process() {

            try {
              counter = (++counter) % 20;
              if (counter == 0) logger.info("Beating...");
              Map<String, Object> data = new HashMap<>();
              data.put("current_cpu_usage",(int) (osBean.getProcessCpuLoad()*100));
              data.put("state", String.valueOf(locust.getState()).toLowerCase());
              transport.send(new Message("heartbeat", data, -1, Beat.this.locust.nodeID));
            } catch (Exception e) {
              return Action.BREAK;
            }

            return Action.CONTINUE;
          }
        };
    logger.info("Initialized");
  }
}
