package com.alecor.batch.thread;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author yuan_kf
 * @ClassName BatchAbortPolicy
 * @date 2021/4/23 16:16
 * @Description  批处理任务的拒绝策略
 * @Version V1.0
 */

public class BatchAbortPolicy implements RejectedExecutionHandler {
    
    private final LongAdder counter = new LongAdder();
    
    @Override
    public void rejectedExecution(Runnable runnable, ThreadPoolExecutor executor) {
        if (runnable instanceof BatchRunable && ((BatchRunable)runnable).isForceExecution()) {
            BlockingQueue<Runnable> queue =  executor.getQueue();
            if (!(queue instanceof BlockingQueue)) {
                throw new IllegalStateException("forced execution, but expected a size queue");
            } else {
                try {
                    queue.put(runnable);
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("forced execution, but got interrupted", exception);
                }
            }
        } else {
            this.counter.increment();
            throw new BatchRejectedExecutionException("rejected execution of " + runnable + " on " + executor, executor.isShutdown());
        }
    }
    
    public long rejected() {
        return this.counter.sum();
    }
}

