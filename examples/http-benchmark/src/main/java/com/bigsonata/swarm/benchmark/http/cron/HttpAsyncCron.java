package com.bigsonata.swarm.benchmark.http.cron;

import com.bigsonata.swarm.Cron;
import com.bigsonata.swarm.Props;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;

import java.util.concurrent.Executor;

public class HttpAsyncCron extends HttpCron {
    public static Executor executor = null;

    public HttpAsyncCron() {
        super(Props.createAsync().setName(String.format("async-cron")));
    }

    @Override
    public void process() {
        long start = System.currentTimeMillis();
        try {
            ListenableFuture<Response> request = asyncHttpClient.prepareGet(URL).execute();

            request.addListener(
                    () -> {
                        long latency = System.currentTimeMillis() - start;
                        LOGGER.info("Processed in {}ms", latency);
                        recordSuccess(latency);
                    },
                    executor);

        } catch (Exception e) {
            LOGGER.error("Failed to process. Reason: {}", e.getMessage());
            LOGGER.error("Stack: {}", ExceptionUtils.getStackTrace(e));
            long latency = System.currentTimeMillis() - start;
            recordFailure(latency, e.getMessage());
        }
    }

    @Override
    public Cron clone() {
        return new HttpAsyncCron();
    }
}
