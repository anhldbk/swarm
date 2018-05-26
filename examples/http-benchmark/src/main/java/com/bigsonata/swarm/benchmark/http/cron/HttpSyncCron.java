package com.bigsonata.swarm.benchmark.http.cron;

import com.bigsonata.swarm.Cron;
import com.bigsonata.swarm.Props;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;

public class HttpSyncCron extends HttpCron {
    public HttpSyncCron() {
        super(Props.create().setName(String.format("sync-cron")));
    }

    @Override
    public void process() {
        long start = System.currentTimeMillis(), latency = 0;
        try {
            ListenableFuture<Response> request = asyncHttpClient.prepareGet(URL).execute();
            // blocking wait
            request.get();
            latency = System.currentTimeMillis() - start;
            recordSuccess(latency);
        } catch (Exception e) {
            LOGGER.error("Failed to process. Reason: {}", e.getMessage());
            LOGGER.error("Stack: {}", ExceptionUtils.getStackTrace(e));
            latency = System.currentTimeMillis() - start;
            recordFailure(latency, e.getMessage());
        }
        LOGGER.info("Processed in {}ms", latency);
    }

    @Override
    public Cron clone() {
        return new HttpSyncCron();
    }
}
