package com.alecor.batch.thread;



import com.alecor.batch.scheduler.ScheduledCancellableAdapter;

import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * @author yuan_kf
 * @ClassName BatchScheduler
 * @date 2021/4/23 15:53
 * @Description
 * @Version V1.0
 */

public interface BatchScheduler {
    
    
    /**
     * 初始化一个定时器线程池糖
     *
     * 设定线程数为1
     *
     * @return
     */
    static ScheduledThreadPoolExecutor initScheduler() {
       final ScheduledThreadPoolExecutor scheduler = new SafeScheduledThreadPoolExecutor(1, daemonThreadFactory( "scheduler"), new BatchAbortPolicy());
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        scheduler.setRemoveOnCancelPolicy(true);
        return scheduler;
    }
    
    /**
     * 终止线程
     * @param scheduledThreadPoolExecutor
     * @param timeout
     * @param timeUnit
     * @return
     */
    static boolean terminate(ScheduledThreadPoolExecutor scheduledThreadPoolExecutor, long timeout, TimeUnit timeUnit) {
        scheduledThreadPoolExecutor.shutdown();
        if (awaitTermination(scheduledThreadPoolExecutor, timeout, timeUnit)) {
            return true;
        } else {
            scheduledThreadPoolExecutor.shutdownNow();
            return awaitTermination(scheduledThreadPoolExecutor, timeout, timeUnit);
        }
    }
    
    /**
     * 等待线程终止
     * @param scheduledThreadPoolExecutor
     * @param timeout
     * @param timeUnit
     * @return
     */
    static boolean awaitTermination(ScheduledThreadPoolExecutor scheduledThreadPoolExecutor, long timeout, TimeUnit timeUnit) {
        try {
            if (scheduledThreadPoolExecutor.awaitTermination(timeout, timeUnit)) {
                return true;
            }
        } catch (InterruptedException var5) {
            Thread.currentThread().interrupt();
        }
        return false;
    }
    
    
    static ScheduledCancellable wrapAsScheduledCancellable(ScheduledFuture<?> scheduledFuture) {
        return new ScheduledCancellableAdapter(scheduledFuture);
    }
    
    /**
     * 创建守护线程
     * @param namePrefix
     * @return
     */
    public static ThreadFactory daemonThreadFactory(String namePrefix) {
        return new BatchThreadFactory(namePrefix);
    }
    
    static class BatchThreadFactory implements ThreadFactory {
        final ThreadGroup group;
        final AtomicInteger threadNumber = new AtomicInteger(1);
        final String namePrefix;
    
        BatchThreadFactory(String namePrefix) {
            this.namePrefix = namePrefix;
            SecurityManager s = System.getSecurityManager();
            this.group = s != null ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        }
        
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(this.group, r, this.namePrefix + "[T#" + this.threadNumber.getAndIncrement() + "]", 0L);
            t.setDaemon(true);
            return t;
        }
    }
    
    /**
     * 自定义线程池，继承定时器线程, 重写了 afterExecute方法
     */
    public class SafeScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {
        
        
        public SafeScheduledThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
            super(corePoolSize, threadFactory, handler);
        }
        public SafeScheduledThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory) {
            super(corePoolSize, threadFactory);
        }
    
        public SafeScheduledThreadPoolExecutor(int corePoolSize) {
            super(corePoolSize);
        }
        
        @Override
        protected void afterExecute(Runnable runnable, Throwable t) {
            if (t == null) {
                if (runnable instanceof RunnableFuture && ((RunnableFuture)runnable).isDone()) {
    
                    if (runnable instanceof RunnableFuture) {
                        assert ((RunnableFuture)runnable).isDone();
        
                        try {
                            ((RunnableFuture)runnable).get();
                        } catch (Exception var3) {
                            
                            if (var3 instanceof InterruptedException) {
                                Thread.currentThread().interrupt();
                            }
                            if (var3 instanceof ExecutionException) {
                                throw new RuntimeException(var3);
                            }
                        }
                    }
                }
            }
        }
    }
    
    ScheduledCancellable schedule(Runnable command, Long interval, String executor);
    
    /**
     *  定时线程
     * @param command
     * @param interval
     * @param executor
     * @return
     */
    public default Cancellable scheduleWithFixedDelay(Runnable command, Long interval, String executor) {
        return new ReschedulingRunnable(command, interval, executor, this, (e) -> {}, (e) -> {});
    }
    
    default Runnable preserveContext(Runnable command) {
        return command;
    }
    
    public interface ScheduledCancellable extends Delayed, Cancellable {
    }

    
    public static final class ReschedulingRunnable extends BatchRunable implements Cancellable {
        private final Runnable runnable;
        private final Long interval;
        private final String executor;
        private final BatchScheduler scheduler;
        private final Consumer<Exception> rejectionConsumer;
        private final Consumer<Exception> failureConsumer;
        private volatile boolean run = true;
        
        public ReschedulingRunnable(Runnable runnable, Long interval, String executor, BatchScheduler scheduler, Consumer<Exception> rejectionConsumer,
                Consumer<Exception> failureConsumer) {
            this.runnable = runnable;
            this.interval = interval;
            this.executor = executor;
            this.scheduler = scheduler;
            this.rejectionConsumer = rejectionConsumer;
            this.failureConsumer = failureConsumer;
            scheduler.schedule(this, interval, executor);
        }
    
        @Override
        public boolean cancel() {
            boolean result = this.run;
            this.run = false;
            return result;
        }
        
        @Override
        public boolean isCancelled() {
            return !this.run;
        }
        
        @Override
        public void doRun() {
            if (this.run) {
                this.runnable.run();
            }
            
        }
        
        @Override
        public void onFailure(Exception e) {
            this.failureConsumer.accept(e);
        }
        
        @Override
        public void onRejection(Exception e) {
            this.run = false;
            this.rejectionConsumer.accept(e);
        }
    
        @Override
        public void onAfter() {
            if (this.run) {
                try {
                    this.scheduler.schedule(this, this.interval, this.executor);
                } catch (BatchRejectedExecutionException exception) {
                    this.onRejection(exception);
                }
            }
        }
        @Override
        public String toString() {
            return "ReschedulingRunnable{runnable=" + this.runnable + ", interval=" + this.interval + '}';
        }
    }
    
}
