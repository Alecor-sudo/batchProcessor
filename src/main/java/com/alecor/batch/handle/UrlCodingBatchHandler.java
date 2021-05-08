package com.alecor.batch.handle;



import com.alecor.batch.BatchRequest;
import com.alecor.batch.exception.BaseException;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yuan_kf
 * @ClassName UrlCodingBatchHandler
 * @date 2021/4/25 10:49
 * @Description
 * @Version V1.0
 */

public class UrlCodingBatchHandler<T> extends BatchHandler  {
    
    public static AtomicInteger count = new AtomicInteger(0);
    
    @Override
    public boolean executeBatch(BatchRequest request) {
        System.out.println("我开始处理数据了，我处理的数据分别是从：");
    
        int data = count.incrementAndGet();
//
//        if(data == 2){
//            System.out.println("处理成功");
//            return true;
//        }
        
        List list = request.getBatchData();
        for (int i = 0; i < list.size(); i++) {
            System.out.println(list.get(i));
        }
//        return true;
        throw  new BaseException("Business_Insert","处理失败");
    }
}
