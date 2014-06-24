package org.shanbo.feluca.node.http;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.shanbo.feluca.util.Strings;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class HttpResponseUtil {
	
	public final static String RESPONSE = "response";
	public final static String REQUEST = "request";
	
	/*
	final static String basicFormat = "{\"message\":\"%s\"}";
	final static String messageFormat = "{\"message\":\"%s\", \"reason\":\"%s\"}";
	
	public static void setHttpResponseWithStatus(DefaultHttpResponse resp , HttpResponseStatus status){
		resp.setStatus( status );
		resp.setContent(ChannelBuffers.copiedBuffer( String.format(basicFormat, status.toString()), 
				Charset.forName("UTF-8")));
	}
	
	public static void setHttpResponseWithMessage(DefaultHttpResponse resp , HttpResponseStatus status, String content){
		resp.setStatus( status );
		resp.setContent(ChannelBuffers.copiedBuffer( String.format(messageFormat, status.toString(), content), 
				Charset.forName("UTF-8")));
	}
	
	

	public static void setServerErrorResp( DefaultHttpResponse resp ){
		setHttpResponseWithStatus(resp, HttpResponseStatus.SERVICE_UNAVAILABLE);
	}
	
	public static void setNotFoundDataResp( DefaultHttpResponse resp ){
		setHttpResponseWithStatus(resp, HttpResponseStatus.NOT_FOUND);
	}
	
	
	public static void setParameterErrorResp( DefaultHttpResponse resp ){
		setHttpResponseWithStatus(resp, HttpResponseStatus.BAD_REQUEST);
	}
	
	public static void setMethodNotAllowedResp( DefaultHttpResponse resp ){
		setHttpResponseWithStatus(resp, HttpResponseStatus.METHOD_NOT_ALLOWED);
	}
	
	public static void setHttpResponseOkReturn( DefaultHttpResponse resp, String content){
		resp.setStatus( HttpResponseStatus.OK );
		resp.setContent(ChannelBuffers.copiedBuffer( content, 
				Charset.forName("UTF-8")));
	}
	
	public static void setHttpResponseOkReturn( DefaultHttpResponse resp, byte[] content){
		resp.setStatus( HttpResponseStatus.OK );
		resp.setContent(ChannelBuffers.copiedBuffer( content));
	}
	*/
	
	public static void setResponse( DefaultHttpResponse resp, Object respHead, Object respBody){
		setResponse(resp, respHead, respBody, HttpResponseStatus.OK);
	}
	
	public static void setExceptionResponse(DefaultHttpResponse resp, Object respHead, String errorMsg, Throwable e){
		String exception = Strings.throwableToString(e);
		setResponse(resp, respHead, errorMsg + "\n" + exception, HttpResponseStatus.INTERNAL_SERVER_ERROR);
	}
	
	
	public static void setResponse( DefaultHttpResponse resp, Object respHead, Object respBody, HttpResponseStatus status){
		resp.setStatus( status );
		JSONObject json = new JSONObject();
		if (respHead instanceof JSONObject || respHead instanceof JSONArray ){
			json.put(REQUEST, respHead);
		}else
			json.put(REQUEST , respHead.toString());
		if (respBody instanceof JSONObject || respBody instanceof JSONArray)
			json.put(RESPONSE, respBody);
		else {
			json.put(RESPONSE , respBody.toString());
		}
		
		resp.setContent(ChannelBuffers.copiedBuffer( json.toString().getBytes()));
	}
	
	
	
}
