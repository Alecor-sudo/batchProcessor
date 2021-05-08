package com.alecor.batch.Listener;


/**
 * @author yuan_kf
 * @ClassName BatchListerner
 * @date 2021/4/25 14:45
 * @Description
 * @Version V1.0
 */

public interface BatchListerner<BatchResponse> {
    
    void onResponse(BatchResponse t);
    
    void onFailure(BatchResponse response, Exception exception);
    
    static <BatchResponse> BatchListerner<BatchResponse> runAfter(final BatchListerner<BatchResponse> delegate, final Runnable runAfter) {
        return new BatchListerner<BatchResponse>() {
            @Override
            public void onResponse(BatchResponse response) {
                try {
                    delegate.onResponse(response);
                } finally {
                    runAfter.run();
                }
                
            }
    
            @Override
            public void onFailure(BatchResponse response, Exception exception) {
                try {
                    delegate.onFailure(response,exception);
                } finally {
                    runAfter.run();
                }
            }
        };
    }
    
}
