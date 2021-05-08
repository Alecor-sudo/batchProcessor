package com.alecor.batch.thread;

import java.util.concurrent.RejectedExecutionException;

/**
 * @author yuan_kf
 * @ClassName BatchRejectedExecutionException
 * @date 2021/4/23 17:23
 * @Description 批处理拒绝异常
 * @Version V1.0
 */

public class BatchRejectedExecutionException extends RejectedExecutionException {
    
    private final boolean isExecutorShutdown;
    
    public BatchRejectedExecutionException(String message, boolean isExecutorShutdown) {
        super(message);
        this.isExecutorShutdown = isExecutorShutdown;
    }
    
    public BatchRejectedExecutionException(String message) {
        this(message, false);
    }
    
    public BatchRejectedExecutionException() {
        this((String) null, false);
    }
    
    public boolean isExecutorShutdown() {
        return this.isExecutorShutdown;
    }
    
}