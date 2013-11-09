package org.shanbo.feluca.common;

/**
 * @author liangguoning
 * @version 1.0
 * 
 * 
 *          知道明确的错误原因的异常
 */
public class SearchException extends Exception {
	private static final long serialVersionUID = 1L;
	public int errorCode=502;
	public SearchException(String message) {
		super(message);
	}

	public SearchException(String message, Throwable cause) {
		super(message, cause);
	}
	public SearchException(String message,int errcode) {
		super(message);
		this.errorCode=errcode;
	}

	public SearchException(String message, Throwable cause,int errcode) {
		super(message, cause);
	    this.errorCode=errcode;
	}
	
}
