package com.alecor.batch.thread;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * @author yuan_kf
 * @ClassName BatchBackoffPolicy
 * @date 2021/4/24 21:56
 * @Description 批处理补偿模式
 * @Version V1.0
 */

public class BatchBackoffPolicy implements Iterable<Long> {
    
    private static final BatchBackoffPolicy NO_BACKOFF = new NoBackoff();
    
    public static BatchBackoffPolicy noBackoff() {
        return NO_BACKOFF;
    }
    
    public static BatchBackoffPolicy exponentialBackoff() {
        return exponentialBackoff(50L, 8);
    }
    
    /**
     *
     * @param initialDelay 重试延迟时间，最大重试次数
     * @param maxNumberOfRetries
     * @return
     */
    public static BatchBackoffPolicy exponentialBackoff(Long initialDelay, int maxNumberOfRetries) {
        return new ExponentialBackoff(checkDelay(initialDelay), maxNumberOfRetries);
    }
    
    private static int checkDelay(Long delay) {
        // 不能超过Int最大值
        if (delay >  Integer.MAX_VALUE) {
            throw new IllegalArgumentException("delay must be <= 2147483647 ms");
        } else {
            return  delay.intValue();
        }
    }
    
    
    @Override
    public Iterator<Long> iterator() {
        return null;
    }
    
    @Override
    public void forEach(Consumer<? super Long> action) {
    
    }
    
    @Override
    public Spliterator<Long> spliterator() {
        return null;
    }
    
    private static class ExponentialBackoff extends BatchBackoffPolicy {
        private final int start;
        private final int numberOfElements;
        
        private ExponentialBackoff(int start, int numberOfElements) {
            assert start >= 0;
            assert numberOfElements >= 0;
            
            this.start = start;
            this.numberOfElements = numberOfElements;
        }
        
        @Override
        public Iterator<Long> iterator() {
            return new ExponentialBackoffIterator(this.start, this.numberOfElements);
        }
    }
    
    private static class ExponentialBackoffIterator implements Iterator<Long> {
        private final int numberOfElements;
        private final int start;
        private int currentlyConsumed;
        
        private ExponentialBackoffIterator(int start, int numberOfElements) {
            this.start = start;
            this.numberOfElements = numberOfElements;
        }
        
        @Override
        public boolean hasNext() {
            return this.currentlyConsumed < this.numberOfElements;
        }
        
        @Override
        public Long next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException("Only up to " + this.numberOfElements + " elements");
            } else {
                // 根据指数计算，获取延迟时间
                int result = this.start + 10 * ((int)Math.exp(0.8D * (double)this.currentlyConsumed) - 1);
                ++this.currentlyConsumed;
                return (long)result;
            }
        }
    }
    
    /**
     * 不需要补偿
     */
    private static class NoBackoff extends BatchBackoffPolicy {
        
        private NoBackoff() {
        }
        
        @Override
        public Iterator<Long> iterator() {
            return new Iterator<Long>() {
                @Override
                public boolean hasNext() {
                    return false;
                }
                
                @Override
                public Long next() {
                    throw new NoSuchElementException("No backoff");
                }
            };
        }
    }

}
