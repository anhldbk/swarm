package com.bigsonata.swarm.interop;

import com.bigsonata.swarm.Context;
import com.bigsonata.swarm.common.whisper.DisruptorBroker;
import com.bigsonata.swarm.common.whisper.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMonitor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * ZeroMQ transport for Locust
 */
public abstract class ZeroTransport extends Transport {
  private static final Logger logger = LoggerFactory.getLogger(ZeroTransport.class.getCanonicalName());
  private final int port;
  private final String host;
  private final String nodeId;
  private final String addr;
  AtomicReference<State> state = new AtomicReference<State>(Transport.State.DISCONNECTED);
  private LoopingThread receiver;
  private ZContext zeroContext = null;
  private ZMQ.Socket commSocket;
  private CountDownLatch waiter = new CountDownLatch(1);
  private DisruptorBroker sender;
  private TransportMonitor monitor;

  protected ZeroTransport(Context ctx) {
    this.host = ctx.getMasterHost();
    this.port = ctx.getMasterPort();
    this.addr = String.format("tcp://%s:%d", host, port);
    this.nodeId = ctx.getNodeId();
  }

  private void initializeSender() {
    logger.info("Initializing sender...");
    try {
      if (sender != null) {
        return;
      }
      MessageHandler<Message> messageHandler = (s, message) -> {
        if (message == null) {
          return;
        }
        byte[] bytes = message.getBytes();
        try {
          if (!commSocket.send(bytes)) {
            logger.error("Can NOT send");
            return;
          }
        } catch (Exception ex) {
          return;
        }
        if (message.isQuit()) {
          waiter.countDown();
        }
      };
      this.sender = DisruptorBroker.Builder.newInstance().setMessageHandler(messageHandler).build();
      this.sender.initialize();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void initializeMonitor() {
    logger.info("Initializing monitor...");
    logger.info("> host={}", host);
    logger.info("> port={}", port);
    int interval = 100; // ms
    this.monitor = new TransportMonitor(interval);
  }

  private void initializeReceiver() {
    logger.info("Initializing receiver...");
    if (this.receiver != null) {
      this.receiver.restart();
      return;
    }

    this.receiver = new LoopingThread("locust-receiver") {
      @Override
      public Action process() throws Exception {
        try {
          byte[] bytes = ZeroTransport.this.commSocket.recv();
          if (bytes != null) {
            ZeroTransport.this.onMessage(new Message(bytes));
            return Action.CONTINUE;
          }
        } catch (Exception ex) {
//          ex.printStackTrace();
        }
        return Action.BREAK; // terminate this loop
      }
    };
  }

  public void onConnected() {
    if (state.get() != Transport.State.DISCONNECTED) {
      return;
    }
    logger.info("Connected to Locust master.");
    state.set(Transport.State.CONNECTED);
    bootstrap();
  }

  public void onDisconnected() {
    if (state.get() != Transport.State.CONNECTED) {
      return;
    }
    logger.warn("Can NOT connect to Locust master. Waiting...");
    state.set(Transport.State.DISCONNECTED);
  }

  private void release() {
    if (zeroContext == null) {
      return;
    }
    try {
      logger.info("Releasing network resources...");
      if (commSocket != null) {
        commSocket.close();
        commSocket = null;
      }
      if (zeroContext != null) {
        zeroContext.close();
        zeroContext = null;
      }
    } finally {

    }
  }

  private void bootstrap() {
    logger.info("Bootstrapping ...");
    logger.info(" > host={}", host);
    logger.info(" > port={}", port);
    release();
    boolean success = true;
    zeroContext = new ZContext(3);
    commSocket = zeroContext.createSocket(ZMQ.DEALER);
    commSocket.setIdentity(nodeId.getBytes());
    success &= commSocket.connect(addr);
    if (!success) {
      logger.error("Can NOT connect to communication socket");
      return;
    }
    initializeReceiver();
    logger.info("Bootstrapped");
  }

  public void initialize() throws Exception {
    logger.info("Initializing...");
    initializeSender();
    initializeMonitor();
    logger.info("Initialized");
  }

  public void send(Message message) throws Exception {
    if (this.commSocket == null) {
      logger.warn("Please bootstrap() first");
      return;
    }
    if (state.get() == Transport.State.DISCONNECTED) {
      logger.error("Can NOT send messages");
      return;
    }
    if (sender == null) {
      throw new Exception("Must initialize first");
    }
    this.sender.produce(null, message);

  }

  @Override
  public void dispose() {
    // REMEMBER: to gracefully dispose, you must send `quit` messages first
    logger.info("Disposing...");
    try {
      this.state.set(Transport.State.CLOSING);
      if (this.receiver != null) {
        logger.info("Disposing receiver...");
        this.receiver.dispose();
      }
      if (this.sender != null) {
        logger.info("Disposing sender...");
        this.sender.dispose();
      }


      this.monitor.dispose();
      release();
      this.state.set(Transport.State.CLOSED);
      logger.info("Disposed");

    } catch (Exception e) {

    }
  }

  class TransportMonitor extends LoopingThread {
    ZMQ.Socket socket;
    private State state;
    private ZMonitor monitor;

    public TransportMonitor(int interval) {
      super("locust-monitor", interval);
      state = Transport.State.DISCONNECTED;
    }

    public TransportMonitor() {
      this(100);
    }

    ZMonitor.ZEvent getNextEvent() {
      if (monitor == null) {

        ZContext context = new ZContext(1);
        socket = context.createSocket(ZMQ.DEALER);
        socket.setIdentity((nodeId + "-monitor").getBytes());
        monitor = new ZMonitor(context, socket);
        monitor.add(ZMonitor.Event.ALL);
        monitor.start();

        boolean connected = socket.connect(addr);
        if (!connected) {
          logger.warn("Could not connect...");
        }
      }

      return monitor.nextEvent(true);
    }

    @Override
    public Action process() throws Exception {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          ZMonitor.ZEvent event = getNextEvent();
          if (event == null) {
            //            tryReconnect();
            continue;
          }
          logger.info("Transport event = {}", event.type);
          if (event.type.compareTo(ZMonitor.Event.CONNECTED) == 0) {
            if (state == Transport.State.DISCONNECTED) {
              ZeroTransport.this.onConnected();
              state = Transport.State.CONNECTED;
            }
            continue;
          }
          if (event.type.compareTo(ZMonitor.Event.DISCONNECTED) == 0) {
            if (state == Transport.State.CONNECTED) {
              ZeroTransport.this.onDisconnected();
              state = Transport.State.DISCONNECTED;
            }
          }
        }
      } finally {
        return Action.BREAK;
      }
    }

  }
}
