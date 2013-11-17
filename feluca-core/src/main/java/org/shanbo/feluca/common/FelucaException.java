package org.shanbo.feluca.common;

/**
 * @author liangguoning
 * @version 1.0
 * 
 * 
 *          知道明确的错误原因的异常
 */
public class FelucaException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	public int errorCode=502;
	public FelucaException(String message) {
		super(message);
	}

	public FelucaException(String message, Throwable cause) {
		super(message, cause);
	}
	public FelucaException(String message,int errcode) {
		super(message);
		this.errorCode=errcode;
	}

	public FelucaException(String message, Throwable cause,int errcode) {
		super(message, cause);
	    this.errorCode=errcode;
	}
	
}
