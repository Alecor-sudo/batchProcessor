package com.alecor.batch.Listener;


import com.alecor.batch.BatchRequest;

/**
 * @author yuan_kf
 * @ClassName BatchResponse
 * @date 2021/4/25 14:52
 * @Description
 * @Version V1.0
 */

public class BatchResponse {
    
    private BatchRequest request;
    private boolean isSuccess;
    
    public BatchResponse(BatchRequest request, boolean isSuccess){
       this.isSuccess = !isSuccess;
       this.request = request;
    }
    
    public BatchRequest getBatchRequest() {
        return this.request;
    }
    
    public boolean hasSuccess() {
        return isSuccess;
    }
    
    
    public static class Builder{
    
        private BatchRequest request;
        private boolean isSuccess;
    
        public Builder setRequest(BatchRequest request) {
            this.request = request;
            return this;
        }
    
        public Builder setSuccess(boolean success) {
            isSuccess = success;
            return this;
        }
        public BatchResponse build(){
            return new BatchResponse(this.request,this.isSuccess);
        }
    }
}
