package com.alecor.batch;


import com.alecor.batch.Listener.BatchResponse;
import com.alecor.batch.handle.BatchHandler;
import com.alecor.batch.handle.BatchRequestHandler;
import com.alecor.batch.thread.BatchBackoffPolicy;
import com.alecor.batch.thread.BatchScheduler;
import com.alecor.batch.thread.Cancellable;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * @author yuan_kf
 * @ClassName BatchProcessor
 * @date 2021/4/23 09:41
 * @Description 通用批处理方法
 *
 * 1、支持定时处理
 * 2、支持阈值处理
 * 3、支持重试机制
 *
 * 使用方法见 test.class
 *
 * @Version V1.0
 */

public class BatchProcessor<T> implements Closeable {
    
    /**
     * 监听接口实现
     * @param <T>
     */
    public interface Listener<T> {
        
        void beforBatch(BatchRequest<T> t);
        
        void afterBatch(BatchResponse response);
        
        void afterBatch(Exception exception, BatchResponse response);
    }
    
    
    /**
     * 最大执行个数
     */
    private final int batchSize;
    
    /**
     * 数据存储容器
     */
    private BatchRequest<T> requests;
    
    /**
     * BatchRequest新对象
     */
    private final Supplier<BatchRequest<T>> batchRequestSupplier;
    
    /**
     * 可重入锁机制
     */
    private final ReentrantLock lock;
    
    /**
     * 批任务处理器
     */
    private final BatchRequestHandler batchRequestHandler;
    
    /**
     * 刷新机制
     */
    private final Cancellable cancellableFlushTask;
    
    /**
     * 任务处理完成接口
     */
    private final Runnable onClose;
    
    /**
     * 线程结束标识
     */
    private volatile boolean closed;
    
    
    /**
     *
     * @param batchSize 允许最大处理数
     * @param flushInterval 刷新间隔时间
     * @param batchHandler 任务处理接口
     * @param listener 任务处理监听接口
     * @param backoffPolicy 重试策略
     * @param flushScheduler 刷新定时器
     * @param retryScheduler 重试定时器
     * @param concurrentRequests 当前任务支持的线程数
     * @param onClose 线程关闭回掉数据
     * @param batchRequestSupplier 数据生产对象
     */
    public BatchProcessor(Integer batchSize, Long flushInterval, BatchHandler batchHandler, Listener listener, BatchBackoffPolicy backoffPolicy,
            BatchScheduler flushScheduler, BatchScheduler retryScheduler, int concurrentRequests, Runnable onClose,
            Supplier<BatchRequest<T>> batchRequestSupplier) {
        this.batchSize = batchSize;
        this.lock = new ReentrantLock();
        this.closed = false;
        this.requests = batchRequestSupplier.get();
        this.batchRequestSupplier = batchRequestSupplier;
        this.batchRequestHandler = new BatchRequestHandler(listener, batchHandler, backoffPolicy, retryScheduler, concurrentRequests);
        this.cancellableFlushTask = this.startFlushTask(flushInterval, flushScheduler);
        this.onClose = onClose;
    }
    
    
    public static Builder builder(BatchHandler batchHandler){
        Objects.requireNonNull(batchHandler, "batchHandler");
        return builder(new Listener() {
            @Override
            public void beforBatch(BatchRequest t) {}
    
            @Override
            public void afterBatch(BatchResponse response) {}
    
            @Override
            public void afterBatch(Exception exception, BatchResponse response) {}
        },batchHandler);
    }
    
    
    public static Builder builder(Listener listener, BatchHandler batchHandler) {
        Objects.requireNonNull(listener, "listener");
        Objects.requireNonNull(batchHandler, "batchHandler");
        ScheduledThreadPoolExecutor flushScheduledThreadPoolExecutor = BatchScheduler.initScheduler();
        ScheduledThreadPoolExecutor retryScheduledThreadPoolExecutor = BatchScheduler.initScheduler();
        return new Builder(listener, batchHandler, buildScheduler(flushScheduledThreadPoolExecutor),
                buildScheduler(retryScheduledThreadPoolExecutor), () -> {
            BatchScheduler.terminate(flushScheduledThreadPoolExecutor, 10L, TimeUnit.SECONDS);
            BatchScheduler.terminate(retryScheduledThreadPoolExecutor, 10L, TimeUnit.SECONDS);
        });
    }
    
    /**
     * 添加处理对象
     * @param t
     * @return
     */
    public BatchProcessor add(T t) {
        this.lock.lock();
        BatchRequest batchRequest = null;
        try {
            this.ensureOpen();
            this.requests.add(t);
            batchRequest = this.newBatchRequestIfNeeded();
            System.out.println("我添加数据了,数据值为:"+ t);
        } finally {
            this.lock.unlock();
        }
        if (batchRequest != null) {
            this.execute(batchRequest);
        }
        
        return this;
    }
    
    private void execute(BatchRequest requests) {
        this.batchRequestHandler.execute(requests);
    }
    
    private void execute() {
        BatchRequest batchRequest = this.requests;
        // 创建新对象
        this.requests = this.batchRequestSupplier.get();
        this.batchRequestHandler.execute(batchRequest);
    }
    
    
    // 判断是否可以执行
    private BatchRequest newBatchRequestIfNeeded() {
        this.ensureOpen();
        if (!this.isOverTheLimit()) {
            return null;
        } else {
            BatchRequest batchRequest = this.requests;
            this.requests = this.batchRequestSupplier.get();
            return batchRequest;
        }
    }
    
    /**
     * 是否超过限定数据
     *
     * @return
     */
    private boolean isOverTheLimit() {
        if (this.batchSize != -1 && this.requests.numberOfActions() >= this.batchSize) {
            return true;
        } else {
           return false;
        }
    }
    
    /**
     *
     * 当try块退出的时候，会自动调用下列方法
     *
     * 资源自动关闭方法
     */
    @Override
    public void close() {
        try {
            this.awaitClose(0L, TimeUnit.NANOSECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * 资源关闭
     * @param timeout
     * @param unit
     * @return
     * @throws InterruptedException
     */
    public boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        try {
            lock.lock();
            if (closed) {
                return true;
            }
            closed = true;
            this.cancellableFlushTask.cancel();
            
            if (requests.numberOfActions() > 0) {
                execute();
            }
            try {
                return this.batchRequestHandler.awaitClose(timeout, unit);
            } finally {
                onClose.run();
            }
        } finally {
            lock.unlock();
        }
    }
    
    boolean isOpen() {
        return !this.closed;
    }
    
    protected void ensureOpen() {
        if (this.closed) {
            throw new IllegalStateException("Batch process already closed");
        }
    }
    
  
    
    /**
     * 创建定时任务
     *
     * @param scheduledThreadPoolExecutor
     * @returnBatchTreadPool
     */
    private static BatchScheduler buildScheduler(ScheduledThreadPoolExecutor scheduledThreadPoolExecutor) {
        return (command, delay, executor) ->
                BatchScheduler.wrapAsScheduledCancellable(scheduledThreadPoolExecutor.schedule(command,delay, TimeUnit.MILLISECONDS));
    }
    
    
    /**
     * 开始任务
     *
     * @param flushInterval
     * @param scheduler
     * @return
     */
    private Cancellable startFlushTask(Long flushInterval, BatchScheduler scheduler) {
        if (flushInterval == null) {
            return new Cancellable() {
                @Override
                public boolean cancel() {
                    return false;
                }
                
                @Override
                public boolean isCancelled() {
                    return true;
                }
            };
        } else {
            Runnable flushRunnable =  scheduler.preserveContext(new Flush());
            return scheduler.scheduleWithFixedDelay(flushRunnable, flushInterval, "generic");
        }
    }
    
    
    /**
     * 刷新任务处理器
     */
    class Flush implements Runnable {
        @Override
        public void run() {
            lock.lock();
            try {
                if (closed) {
                    return;
                }
                if (requests.numberOfActions() == 0) {
                    return;
                }
                System.out.println("我准备触发flush数据了");
                execute();
            } finally {
                lock.unlock();
            }
        }
    }
    
    /**
     * Flush pending delete
     */
    public void flush() {
        lock.lock();
        try {
            ensureOpen();
            if (this.requests.numberOfActions() > 0) {
                execute();
            }
        } finally {
            lock.unlock();
        }
    }
    
    public static class Builder<T> {
        
        private final Listener listener;
        
        // 刷新定时器
        private final BatchScheduler flushScheduler;
        
        private final BatchScheduler retryScheduler;
        
        // 任务关闭回掉
        private final Runnable onClose;
        
        // 处理条数
        private Integer batchSize;
        
        // 线程执行数(默认为1)
        private int concurrentRequests = 1;
        
        // 执行间隔时间
        private Long flushInterval;
        
        // 批处理处理程序接口
        private BatchHandler handler;
        
        // 补偿策略
        private BatchBackoffPolicy batchBackoffPolicy = BatchBackoffPolicy.exponentialBackoff();
        
        public Builder(Listener listener, BatchHandler handler, BatchScheduler flushScheduler, BatchScheduler retryScheduler, Runnable onClose) {
            this.listener = listener;
            this.batchSize = 1000;
            this.flushInterval = null;
            this.handler = handler;
            this.flushScheduler = flushScheduler;
            this.retryScheduler = retryScheduler;
            this.onClose = onClose;
        }
        
        public Builder setBatchSize(Integer batchSize) {
            this.batchSize = batchSize;
            return this;
        }
        
        public Builder setFlushInterval(Long flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }
        
        public Builder setConcurrentRequests(int concurrentRequests) {
            this.concurrentRequests = concurrentRequests;
            return this;
        }
        
        public Builder setBackoffPolicy(BatchBackoffPolicy backoffPolicy) {
            if (backoffPolicy == null) {
                throw new NullPointerException("'backoffPolicy' must not be null. To disable backoff, pass BackoffPolicy.noBackoff()");
            }
            this.batchBackoffPolicy = backoffPolicy;
            return this;
        }
        
        
        public BatchProcessor build() {
            return new BatchProcessor(this.batchSize, this.flushInterval, this.handler, this.listener, this.batchBackoffPolicy, this.flushScheduler,
                    this.retryScheduler, this.concurrentRequests, this.onClose, createBatchRequest());
        }
        
        private Supplier<BatchRequest<T>> createBatchRequest() {
            return () -> new BatchRequest<T>();
        }
    }
    
    
}
