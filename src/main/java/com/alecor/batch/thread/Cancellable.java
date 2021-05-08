package com.alecor.batch.thread;

/**
 * @author yuan_kf
 * @ClassName Cancellable
 * @date 2021/4/27 22:11
 * @Description
 * @Version V1.0
 */

public interface Cancellable {
    /**
     * 取消
     * @return
     */
    boolean cancel();
    
    /**
     * 已经取消
     * @return
     */
    boolean isCancelled();
}
