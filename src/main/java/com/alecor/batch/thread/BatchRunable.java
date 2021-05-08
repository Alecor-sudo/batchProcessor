package com.alecor.batch.thread;

/**
 * @author yuan_kf
 * @ClassName BatchRunable
 * @date 2021/4/27 15:54
 * @Description
 * @Version V1.0
 */

public abstract class BatchRunable implements Runnable {
 
    public boolean isForceExecution() {
        return false;
    }
    
    @Override
    public final void run() {
        try {
            doRun();
        } catch (Exception t) {
            onFailure(t);
        } finally {
            onAfter();
        }
    }
    
    public void onAfter() {
        // nothing by default
    }
    
 
    public abstract void onFailure(Exception e);
    
    public void onRejection(Exception e) {
        onFailure(e);
    }
    
    protected abstract void doRun() throws Exception;
}
