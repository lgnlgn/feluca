package org.shanbo.feluca.common.http;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;

public class NettyHttpRequest
{

	private final org.jboss.netty.handler.codec.http.HttpRequest request;

	private final Map<String, String> params;

	private final String path;

	private static final Pattern commaPattern = Pattern.compile(",");
	
	private MessageEvent messageEvent;

	public NettyHttpRequest(org.jboss.netty.handler.codec.http.HttpRequest request)
	{
		this.request = request;
		this.params = new HashMap<String, String>();

		String uri = request.getUri();
		int pathEndPos = uri.indexOf('?');
		if (pathEndPos < 0)
		{
			this.path = uri;
		}
		else
		{
			this.path = uri.substring(0, pathEndPos);
			decodeQueryString(uri, pathEndPos + 1, params);
		}
	}

	public HttpMethod method()
	{
		return this.request.getMethod();
	}
	 
	public static void decodeQueryString(String queryString, int fromIndex, Map<String, String> params)
	{
		if (fromIndex < 0)
		{
			return;
		}
		if (fromIndex >= queryString.length())
		{
			return;
		}
		int toIndex;
		while ((toIndex = queryString.indexOf('&', fromIndex)) >= 0)
		{
			int idx = queryString.indexOf('=', fromIndex);
			if (idx < 0)
			{
				continue;
			}
			if(idx>= toIndex)
			{
			    fromIndex = toIndex + 1;
				continue;
			}
			params.put(decodeComponent(queryString.substring(fromIndex, idx)), decodeComponent(queryString.substring(idx + 1, toIndex)));
			fromIndex = toIndex + 1;
		}
		int idx = queryString.indexOf('=', fromIndex);
		if (idx < 0)
		{
			return;
		}
		params.put(decodeComponent(queryString.substring(fromIndex, idx)), decodeComponent(queryString.substring(idx + 1)));
	}
	/**
	 * 编码是否有效
	 * @param text
	 * @return
	 */
	static private boolean Utf8codeCheck(String text){
		StringBuilder sign = new StringBuilder("");
		if (text.startsWith("%e")){
			int p = 0;
//			for (int i = 0, p = 0; p != -1; i++) {
			while( p != -1 ){
				p = text.indexOf("%", p);
				if (p != -1)
					p++;
				sign.append(p);
			}
		}
		return sign.toString().equals("147-1");
	}
	
	/**
	 * 是否Utf8Url编码
	 * @param text the request url
	 * @return boolean if encoding of utf8 return true, else return false
	 */
	static public boolean isUtf8Url(String text) {
		text = text.toLowerCase();
		int p = text.indexOf("%");
		if (p != -1 && text.length() - p > 9) {
			text = text.substring(p, p + 9);
		}
		return Utf8codeCheck(text);
	}
	private static String decodeComponent(String s)
	{
		if (s == null)
		{
			return "";
		}
		try
		{
//			if(isUtf8Url(s))
				return decode(s, "UTF8");
//			else
//			 return decode(s, "GBK");
		}
		catch (UnsupportedEncodingException e)
		{
			throw new UnsupportedCharsetException("UTF8");
		}
	}


	public static String decode(String s, String enc)
		throws UnsupportedEncodingException
	{

		boolean needToChange = false;
		int numChars = s.length();
		StringBuffer sb = new StringBuffer(numChars > 500?
										   numChars / 2:
										   numChars);
		int i = 0;

		if (enc.length() == 0)
		{
			throw new UnsupportedEncodingException("URLDecoder: empty string enc parameter");
		}

		char c;
		byte[] bytes = null;
		while (i < numChars)
		{
			c = s.charAt(i);
			switch (c)
			{
				case '+':
					sb.append(' ');
					i++;
					needToChange = true;
					break;
				case '%':
					/*
		 * Starting with this instance of %, process all
		 * consecutive substrings of the form %xy. Each
		 * substring %xy will yield a byte. Convert all
		 * consecutive  bytes obtained this way to whatever
		 * character(s) they represent in the provided
		 * encoding.
		 */
					if(s.charAt(i+1)=='u')
					{
					      sb.append((char)(Integer.parseInt(s.substring(i+2, i+6),16)));
					    needToChange = true;
					    i=i+6;
					    
					    break;
					}
					else
					{
					    int cc=Integer.parseInt(s.substring(i + 1, i + 3), 16);
						if(cc<128)
						{
						    sb.append((char)cc);
							i+=3;
						    needToChange = true;
							break;
						}
					}

					try
					{

						// (numChars-i)/3 is an upper bound for the number
						// of remaining bytes
						if (bytes == null)
							bytes = new byte[(numChars - i) / 3];
						int pos = 0;
						
//						boolean bk=false;

						while (((i + 2) < numChars) && (c == '%'))
						{
						   int cc=Integer.parseInt(s.substring(i + 1, i + 3), 16);
							 
							
							bytes[pos++] = (byte)cc ;
							i += 3;
							if (i < numChars)
								c = s.charAt(i);
						}
//						if(bk)
//						{
//							break;
//						}

						// A trailing, incomplete byte encoding such as
						// "%x" will cause an exception to be thrown

						if ((i < numChars) && (c == '%'))
							throw new IllegalArgumentException("URLDecoder: Incomplete trailing escape (%) pattern");

						sb.append(new String(bytes, 0, pos, enc));
					}
					catch (NumberFormatException e)
					{
						throw new IllegalArgumentException("URLDecoder: Illegal hex characters in escape (%) pattern - " + e.getMessage());
					}
					needToChange = true;
					break;
				default:
					sb.append(c);
					i++;
					break;
			}
		}

		return (needToChange?
				sb .toString():
				s);
	}


	public float paramAsFloat(String key, float defaultValue)
	{
		String sValue = param(key);
		if (sValue == null)
		{
			return defaultValue;
		}

		return Float.parseFloat(sValue);

	}

	public int paramAsInt(String key, int defaultValue)
	{
		String sValue = param(key);
		if (sValue == null)
		{
			return defaultValue;
		}

		return Integer.parseInt(sValue);

	}

	public boolean paramAsBoolean(String key, boolean defaultValue)
	{
		return Boolean.parseBoolean(param(key));
	}

	public Boolean paramAsBoolean(String key, Boolean defaultValue)
	{
		String sValue = param(key);
		if (sValue == null)
		{
			return defaultValue;
		}
		return !(sValue.equals("false") || sValue.equals("0") || sValue.equals("off"));
	}

	// public TimeValue paramAsTime(String key, TimeValue defaultValue) {
	// return parseTimeValue(param(key), defaultValue);
	// }
	//
	// public SizeValue paramAsSize(String key, SizeValue defaultValue) {
	// return parseSizeValue(param(key), defaultValue);
	// }

	public String[] paramAsStringArray(String key, String[] defaultValue)
	{
		String value = param(key);
		if (value == null)
		{
			return defaultValue;
		}
		return commaPattern.split(value);
	}

	public String uri()
	{
		return request.getUri();
	}

	public String path()
	{
		return path;
	}

	public Map<String, String> params()
	{
		return params;
	}

	public boolean hasContent()
	{
		return request.getContent().readableBytes() > 0;
	}

	public int contentLength()
	{
		return request.getContent().readableBytes();
	}

	public boolean contentUnsafe()
	{
		return request.getContent().hasArray();
	}

	public int contentByteArrayOffset()
	{
		if (request.getContent().hasArray())
		{
			// get the array offset, and the reader index offset within it
			return request.getContent().arrayOffset() + request.getContent().readerIndex();
		}
		return 0;
	}

	private static Charset UTF8 = Charset.forName("UTF-8");

	public String contentAsString()
	{
		return request.getContent().toString(UTF8);
	}

	public Set<String> headerNames()
	{
		return request.getHeaderNames();
	}

	public String header(String name)
	{
		return request.getHeader(name);
	}

	public List<String> headers(String name)
	{
		return request.getHeaders(name);
	}

	public String cookie()
	{
		return request.getHeader(HttpHeaders.Names.COOKIE);
	}

	public boolean hasParam(String key)
	{
		return params.containsKey(key);
	}

	public String param(String key)
	{
		return params.get(key);
	}

	public String param(String key, String defaultValue)
	{
		String value = params.get(key);
		if (value == null)
		{
			return defaultValue;
		}
		return value;
	}

	public HttpRequest getRequest()
	{
		return request;
	}
	
	public static void main(String [] ss)
	{
//		Map<String,String> m=new HashMap<String,String>();
//
//		try
//		{
//			String s=decode("http%3A//club.woyo.com/club-show%3Fid%3D14938",
//							     "UTF-8");
//			System.out.println(s);;
//		}
//		catch (UnsupportedEncodingException e)
//		{
//			e.printStackTrace();
//		}
////		for(Map.Entry<String,String> e:m.entrySet())
////		{
////			System.out.println(e.getKey()+"--"+e.getValue());
////		}
		Map<String,String> m=new HashMap<String,String>();
	String s=	"http://www.google.com.hk/search?hl=zh-CN&newwindow=1&safe=strict&&sa=X&ei=uuXHTLbUKpHmvQO_ybTEDw&ved=0CBkQBSgA&q=woyo.com&spell=1";
	    try{
	        NettyHttpRequest.decodeQueryString(s, s.indexOf("?") + 1, m);
			System.out.println(m);
	    }catch(Exception e)
	    {
	        e.printStackTrace();
	  
	    }
	    
	}

	public void setMessageEvent(MessageEvent messageEvent)
	{
		this.messageEvent = messageEvent;
	}

	public MessageEvent getMessageEvent()
	{
		return messageEvent;
	}
}
