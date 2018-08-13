package com.bigsonata.swarm.benchmark.http.cron;

import com.bigsonata.swarm.Cron;
import com.bigsonata.swarm.Props;
import com.bigsonata.swarm.benchmark.http.Registry;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Dsl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.asynchttpclient.Dsl.config;

public abstract class HttpCron extends Cron {
    public static final int TIMEOUT = 20000;
    static final String URL = Registry.ENTRY_URL.valueString("https://bigsonata.com");
    private static int MAX = 100;
    private static int CRON_IO_THREADS = Registry.ENTRY_CRON_IO_THREADS.valueInt(8);
    static EventLoopGroup eventLoopGroup = new NioEventLoopGroup(CRON_IO_THREADS);
    static final DefaultAsyncHttpClientConfig CONFIG =
            config().setMaxConnections(MAX)
                    .setMaxConnectionsPerHost(MAX)
                    .setRequestTimeout(TIMEOUT)
                    .setConnectTimeout(TIMEOUT)
                    .setEventLoopGroup(eventLoopGroup)
                    .setIoThreadsCount(CRON_IO_THREADS)
                    .build();
    protected final Logger LOGGER;
    AsyncHttpClient asyncHttpClient;
    protected AtomicInteger pending = new AtomicInteger(0);

    public HttpCron(Props props) {
        super(props.setType("http"));
        this.LOGGER = LoggerFactory.getLogger(HttpCron.class.getSimpleName());
    }

    protected void waitUntilNoPending()  {
        while(pending.get() != 0){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    @Override
    public void dispose() {
        waitUntilNoPending();

        if (asyncHttpClient == null) {
            return;
        }
        try {
            asyncHttpClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void initialize() {
        LOGGER.info("Initializing...");
        LOGGER.info("> URL={}", URL);

        asyncHttpClient = Dsl.asyncHttpClient(CONFIG);
    }
}
