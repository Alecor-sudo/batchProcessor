package com.alecor.batch.handle;


import com.alecor.batch.BatchRequest;

/**
 * @author yuan_kf
 * @ClassName BatchHandler
 * @date 2021/4/25 11:08
 * @Description 数据处理接口
 * @Version V1.0
 */

public abstract class BatchHandler<T> {
    public abstract boolean executeBatch(BatchRequest<T> request) throws Exception;
}
