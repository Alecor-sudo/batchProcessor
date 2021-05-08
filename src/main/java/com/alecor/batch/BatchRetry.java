package com.alecor.batch;


import com.alecor.batch.Listener.BatchListerner;
import com.alecor.batch.Listener.BatchResponse;
import com.alecor.batch.exception.BaseException;
import com.alecor.batch.handle.BatchHandler;
import com.alecor.batch.thread.BatchBackoffPolicy;
import com.alecor.batch.thread.BatchScheduler;
import com.alecor.batch.thread.Cancellable;

import java.util.Iterator;
import java.util.logging.LogManager;

/**
 * @author yuan_kf
 * @ClassName Retry
 * @date 2021/4/23 10:49
 * @Description 任务重试机制
 * @Version V1.0
 */

public class BatchRetry {
    
    // 补偿策略
    private final BatchBackoffPolicy backoffPolicy;
    // 定时任务
    private final BatchScheduler scheduler;
   
    
    public BatchRetry(BatchBackoffPolicy backoffPolicy, BatchScheduler scheduler) {
        this.backoffPolicy = backoffPolicy;
        this.scheduler = scheduler;
    }
    
    public void witchBackoff(BatchHandler customer,BatchRequest request, BatchListerner batchListerner){
       RetryHandler retryHandler =  new RetryHandler(this.backoffPolicy,this.scheduler,customer,batchListerner);
       retryHandler.execute(request);
    }
   
    
    static class RetryHandler  implements BatchListerner<BatchResponse> {
//        private static final Logger logger;
        private final BatchHandler customer;
        private final BatchScheduler scheduler;
        private final Iterator<Long> backoff;
        private final long startTimestampNanos;
        private final BatchListerner<BatchResponse> listener;
        private volatile BatchRequest currentBulkRequest;
    
        private volatile Cancellable retryCancellable;
        
        public RetryHandler(BatchBackoffPolicy backoffPolicy, BatchScheduler scheduler, BatchHandler batchHandler,BatchListerner batchListerner) {
            this.backoff = backoffPolicy.iterator();
            this.customer = batchHandler;
            this.scheduler = scheduler;
            this.startTimestampNanos = System.nanoTime();
            this.listener = batchListerner;
        }
    
    
        @Override
        public void onResponse(BatchResponse response) {
            if (!response.hasSuccess()) {
                // 如果失败了
                this.finishHim(response);
            } else if (this.canRetry()) {
                this.retry(response.getBatchRequest());
            } else {
                this.finishHim(response);
            }
        }
        
        
        @Override
        public void onFailure(BatchResponse response,Exception e) {
            //
            if (e instanceof BaseException &&  ((BaseException)e).getExceptionCategory().equals("Business_Insert") && this.backoff.hasNext()) {
                this.retry(this.currentBulkRequest);
            } else {
                try {
                    this.listener.onFailure(response, e);
                } finally {
                    if (this.retryCancellable != null) {
                        this.retryCancellable.cancel();
                    }
                }
            }
            
        }
        
        private void retry(BatchRequest request) {
            assert this.backoff.hasNext();
            
            Long next = this.backoff.next();
            Runnable command = scheduler.preserveContext(() -> this.execute(request));
            retryCancellable = scheduler.schedule(command, next, "same");
        }
        
        
        // 是否继续重试
        private boolean canRetry() {
            if (this.backoff.hasNext()) {
                return true;
            } else {
                return false;
            }
        }
        
        private void finishHim(BatchResponse batchResponse) {
            try {
                this.listener.onResponse(batchResponse);
            } finally {
                if (this.retryCancellable != null) {
                    this.retryCancellable.cancel();
                }
            }
        }
        
        public void execute(BatchRequest request) {
            try {
               this.currentBulkRequest = request;
               boolean isSuccess = this.customer.executeBatch(request);
               this.onResponse(new BatchResponse(request,isSuccess));
            }catch (Exception exception){
                this.onFailure(new BatchResponse(request,false),exception);
            }
        }
        static {
//            logger = LogManager.getLogger(RetryHandler.class);
        }
    }
}
