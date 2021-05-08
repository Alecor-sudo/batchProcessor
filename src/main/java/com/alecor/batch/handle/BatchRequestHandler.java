package com.alecor.batch.handle;


import com.alecor.batch.BatchProcessor;
import com.alecor.batch.BatchRequest;
import com.alecor.batch.BatchRetry;
import com.alecor.batch.Listener.BatchListerner;
import com.alecor.batch.Listener.BatchResponse;
import com.alecor.batch.thread.BatchBackoffPolicy;
import com.alecor.batch.thread.BatchScheduler;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;

/**
 * @author yuan_kf
 * @ClassName BatchRequestHandler
 * @date 2021/4/23 10:48
 * @Description
 * @Version V1.0
 */

public class BatchRequestHandler<T> {
    
    private final BatchProcessor.Listener listener;
    private final  BatchHandler customer;
    // 控制可允许的线程操作
    private final Semaphore semaphore;
    private final BatchRetry retry;
    private final int concurrentRequests ;
    
    
    public BatchRequestHandler(BatchProcessor.Listener listener, BatchHandler customer, BatchBackoffPolicy backoffPolicy, BatchScheduler retryScheduler, int concurrentRequests) {
        assert concurrentRequests >= 0;
        
        this.customer = customer;
//        this.logger = LogManager.getLogger(this.getClass());
        this.listener = listener;
        this.concurrentRequests = concurrentRequests;
        this.retry = new BatchRetry(backoffPolicy, retryScheduler);
        this.semaphore = new Semaphore(concurrentRequests > 0 ? concurrentRequests : 1);
    }

    public void execute(final BatchRequest<T> request) {
        Runnable toRelease = () -> {
        };
        boolean batchRequestSetupSuccessful = false;

        try {
            this.listener.beforBatch(request);
            this.semaphore.acquire();
            Semaphore semaphore = this.semaphore;
            Objects.requireNonNull(semaphore);
            toRelease = semaphore::release;
            CountDownLatch latch = new CountDownLatch(1);
            retry.witchBackoff(this.customer, request,  BatchListerner.runAfter(new BatchListerner<BatchResponse>() {
                @Override
                public void onResponse(BatchResponse response) {
                    BatchRequestHandler.this.listener.afterBatch(response);
                }
                @Override
                public void onFailure(BatchResponse response,Exception e) {
                    System.out.println("onFailure");
                    BatchRequestHandler.this.listener.afterBatch(e, response);
                }
            }, () -> {
                this.semaphore.release();
                latch.countDown();
            }));
            batchRequestSetupSuccessful = true;
            if (this.concurrentRequests == 0) {
                latch.await();
            }
            
            
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
//            this.logger.info(() -> {
//                return new ParameterizedMessage("Batch request {} has been cancelled.", exception);
//            }, exception);
            this.listener.afterBatch(exception, new BatchResponse.Builder().setRequest(request).setSuccess(false).build());
        } catch (Exception exception) {
//            this.logger.warn(() -> {
//                return new ParameterizedMessage("Failed to execute com.alecor.batch request {}.", exception);
//            }, exception);
            this.listener.afterBatch(exception,new BatchResponse.Builder().setRequest(request).setSuccess(false).build());
        } finally {
            if (!batchRequestSetupSuccessful) {
                toRelease.run();
            }
        }
    }

    public boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        if (this.semaphore.tryAcquire(this.concurrentRequests, timeout, unit)) {
            this.semaphore.release(this.concurrentRequests);
            return true;
        } else {
            return false;
        }
    }
    
}
