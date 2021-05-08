package com.alecor.batch.scheduler;



import com.alecor.batch.thread.BatchScheduler;

import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author yuan_kf
 * @ClassName ScheduledCancellableAdapter
 * @date 2021/4/23 18:09
 * @Description 定时器适配模式，提供各种方法
 * @Version V1.0
 */

public class ScheduledCancellableAdapter implements BatchScheduler.ScheduledCancellable {
    
    private final ScheduledFuture<?> scheduledFuture;
    
    public ScheduledCancellableAdapter(ScheduledFuture<?> scheduledFuture) {
        assert scheduledFuture != null;
        
        this.scheduledFuture = scheduledFuture;
    }
    
    @Override
    public long getDelay(TimeUnit unit) {
        return this.scheduledFuture.getDelay(unit);
    }
    
    @Override
    public int compareTo(Delayed other) {
        return -other.compareTo(this.scheduledFuture);
    }
    
    @Override
    public boolean cancel() {
        if (this.scheduledFuture != null) {
            return this.scheduledFuture.cancel(false);
        }
        return false;
    }
    
    @Override
    public boolean isCancelled() {
        return this.scheduledFuture.isCancelled();
    }
    
}
