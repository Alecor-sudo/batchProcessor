package com.alecor.batch.exception;

/**
 * @author yuan_kf
 * @ClassName BaseException
 * @date 2021/5/5 16:48
 * @Description
 * @Version V1.0
 */

public class BaseException extends RuntimeException {
    private static final long serialVersionUID = -8989633207652858038L;
    private String exceptionCategory;
    
    public BaseException() {
    }
    
    public BaseException(String exceptionCategory, String message) {
        super(message);
        this.exceptionCategory = exceptionCategory;
    }
    
    public BaseException(String exceptionCategory, String message, Throwable cause) {
        super(message, cause);
        this.exceptionCategory = exceptionCategory;
    }
    
    public String toString() {
        return "BaseException{exceptionCategory=" + this.exceptionCategory + '}';
    }
    
    public String getExceptionCategory() {
        return this.exceptionCategory;
    }
    
    public void setExceptionCategory(String exceptionCategory) {
        this.exceptionCategory = exceptionCategory;
    }
}
