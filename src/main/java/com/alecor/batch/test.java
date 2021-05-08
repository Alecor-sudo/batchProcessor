package com.alecor.batch;

import com.alecor.batch.Listener.BatchResponse;
import com.alecor.batch.handle.UrlCodingBatchHandler;
import com.alecor.batch.thread.BatchBackoffPolicy;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author yuan_kf
 * @ClassName test
 * @date 2021/4/26 09:52
 * @Description
 * @Version V1.0
 */

public class test {
    
    public static void main(String[] args) throws InterruptedException {
        
        BatchProcessor<String> batchProcessor = BatchProcessor.builder(new BatchProcessor.Listener() {
            @Override
            public void beforBatch(BatchRequest t) {
                System.out.println("123");
            }
    
            @Override
            public void afterBatch(BatchResponse response) {
                System.out.println("111");
            }
    
            @Override
            public void afterBatch(Exception exception, BatchResponse response) {
                System.out.println("244");
        
            }
        }, new UrlCodingBatchHandler())
                .setFlushInterval(3000L).setBatchSize(5).setConcurrentRequests(2)
                .setBackoffPolicy(BatchBackoffPolicy.exponentialBackoff(1000L, 2)).build();
    
    
//        int start = 1000;
//
//        int currentlyConsumed = 4;
//
//        int result = start + 10 * ((int)Math.exp(0.8D * (double)currentlyConsumed) - 1);
//
//        System.out.println(result);
//
//        batchProcessor.close();
//
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(3);

        for (int i = 0; i < 3; i++) {
            int finalI = i;
            executor.execute(() -> {
                try {
                    batchProcessor.add(finalI + "");
                } catch (Exception e) {
                    System.out.println(e);
                }
            });
            Thread.sleep(6000);
            batchProcessor.add("kaka");
//            batchProcessor.awaitClose(0L, TimeUnit.NANOSECONDS);
            batchProcessor.add("coco");

            Thread.sleep(6000);
            System.out.println("ActiveCount:" + executor.getActiveCount());
            System.out.println("CorePoolSize:" + executor.getCorePoolSize());
        }
       
    
       
        
    }
    
}
