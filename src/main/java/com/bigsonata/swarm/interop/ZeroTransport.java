package com.bigsonata.swarm.interop;

import com.bigsonata.swarm.Locust;
import com.bigsonata.swarm.common.whisper.MessageHandler;
import com.bigsonata.swarm.common.whisper.disruptor.DisruptorBroker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMonitor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/** ZeroMQ transport for Locust */
public abstract class ZeroTransport extends Transport {
  private static final Logger logger =
      LoggerFactory.getLogger(ZeroTransport.class.getCanonicalName());
  private final int port;
  private final String host;
  AtomicReference<State> state = new AtomicReference<State>(Transport.State.DISCONNECTED);
  private int checkInterval = 0;
  private LoopingThread receiver;
  private ZMQ.Context zeroContext = null;
  private ZMQ.Socket pushSocket;
  private ZMQ.Socket pullSocket;
  private CountDownLatch waiter = new CountDownLatch(1);
  private DisruptorBroker sender;
  private TransportMonitor monitor;

  protected ZeroTransport(Locust.Context ctx) {
    this.host = ctx.getMasterHost();
    this.port = ctx.getMasterPort();
    if (checkInterval > 0) {
      this.checkInterval = checkInterval;
    }
  }

  private void initializeSender() {
    logger.info("Initializing sender...");
    try {
      if (sender != null) {
        return;
      }
      MessageHandler<Message> messageHandler =
          (s, message) -> {
            if (message == null) {
              return;
            }
            byte[] bytes = message.getBytes();
            try {
              if (!pushSocket.send(bytes)) {
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
      this.sender =
          DisruptorBroker.Builder.<Message>newInstance().setMessageHandler(messageHandler).build();
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
    this.monitor = new TransportMonitor(host, port, interval);
  }

  private void initializeReceiver() {
    logger.info("Initializing receiver...");
    if (this.receiver != null) {
      this.receiver.restart();
      return;
    }

    this.receiver =
        new LoopingThread("locust-receiver") {
          @Override
          public Action process() throws Exception, java.nio.channels.ClosedByInterruptException {
            try {
              byte[] bytes = ZeroTransport.this.pullSocket.recv();
              if (bytes != null) {
                ZeroTransport.this.onMessage(new Message(bytes));
                return Action.CONTINUE;
              }
            } catch (Exception ex) {
              ex.printStackTrace();
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
      if (pushSocket != null) {
        pushSocket.close();
        pushSocket = null;
      }
      if (pullSocket != null) {
        pullSocket.close();
        pullSocket = null;
      }
      if (zeroContext != null) {
        //                zeroContext.close();
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
    zeroContext = ZMQ.context(2);
    pushSocket = zeroContext.socket(ZMQ.PUSH);
    success &= pushSocket.connect(String.format("tcp://%s:%d", host, port));
    if (!success) {
      logger.error("Can NOT connect to push");
      return;
    }

    pullSocket = zeroContext.socket(ZMQ.PULL);
    success &= pullSocket.connect(String.format("tcp://%s:%d", host, port + 1));
    if (!success) {
      logger.error("Can NOT connect to pull");
      return;
    }

    initializeReceiver();
    logger.info("Bootstrapped");
  }

  public void initialize() throws Exception {
    logger.info("Initializing");
    initializeSender();
    initializeMonitor();
    logger.info("Initialized");
  }

  public void send(Message message) throws Exception {
    if (state.get() == Transport.State.DISCONNECTED) {
      logger.error("Can NOT send messages");
      return;
    }
    if (sender == null) {
      throw new Exception("Must initialize first");
    }
    this.sender.produce(null, message);

    // blocking until we send this critical message
    //        if (message.isQuit()) {
    //            logger.info("Waiting a little bit...");
    //            waiter.await(5, TimeUnit.SECONDS);
    //        }
  }

  public void dispose() {
    // REMEMBER: to gracefully dispose, you must send `quit` messages first
    logger.info("Disposing...");
    try {
      this.state.set(Transport.State.CLOSING);
      if (this.sender != null) {
        logger.info("Disposing sender...");
        this.sender.dispose();
      }
      if (this.receiver != null) {
        logger.info("Disposing receiver...");
        this.receiver.dispose();
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
    String addr;
    ZContext ctx;
    private State state;
    private ZMonitor monitor;

    public TransportMonitor(String host, int port, int interval) {
      super("locust-monitor", interval);
      addr = String.format("tcp://%s:%d", host, port);
      state = Transport.State.DISCONNECTED;
    }

    public TransportMonitor(String host, int port) {
      this(host, port, 100);
    }

    @Override
    public void initialize() {
      super.initialize();
      ctx = new ZContext();
      socket = ctx.createSocket(ZMQ.PUSH);
      socket.setReceiveTimeOut(interval);
    }

    @Override
    public Action process() throws Exception {
      try {

        monitor = new ZMonitor(ctx, socket);
        monitor.add(ZMonitor.Event.CONNECTED, ZMonitor.Event.DISCONNECTED);
        monitor.start();

        ZMonitor.ZEvent event;

        socket.connect(addr);

        while (!Thread.currentThread().isInterrupted()) {
          event = monitor.nextEvent(interval);
          if (event == null) {
            continue;
          }
          if (event.type.compareTo(ZMonitor.Event.CONNECTED) == 0) {
            if (state == Transport.State.DISCONNECTED) {
              ZeroTransport.this.onConnected();
              state = Transport.State.CONNECTED;
            }
            continue;
          }
          if (state == Transport.State.CONNECTED) {
            ZeroTransport.this.onDisconnected();
            state = Transport.State.DISCONNECTED;
          }
        }
      } finally {
        return Action.BREAK;
      }
    }

    @Override
    public void dispose() {
      super.dispose();
      try {
        //            monitor.destroy();
        //            ctx.destroySocket(socket);
        //            ctx.destroy();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
