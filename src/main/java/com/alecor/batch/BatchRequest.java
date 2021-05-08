package com.alecor.batch;


import java.util.ArrayList;
import java.util.List;

/**
 * @author yuan_kf
 * @ClassName BatchRequest
 * @date 2021/4/26 10:14
 * @Description 用户处理批处理的对象
 * @Version V1.0
 */

public class BatchRequest<T> {
    
    final List<T> requests = new ArrayList();
    
    private boolean isFailure;
    
    public int numberOfActions() {
        return this.requests.size();
    }
    
    public void setResponseState(boolean isFailure){
        this.isFailure = isFailure;
    }
    
    public BatchRequest add(T t){
        this.requests.add(t);
        return this;
    }
    
    public List<T> getBatchData(){
        return this.requests;
    }
    
}
