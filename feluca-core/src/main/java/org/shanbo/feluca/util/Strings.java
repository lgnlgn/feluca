package org.shanbo.feluca.util;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.FelucaException;

import com.alibaba.fastjson.JSONObject;



public class Strings {
	
	public static final String SPILT_COMMA = ",";
	public static final String CONTENT_TYPE = "application/json; charset=UTF-8";
	
	public static final String INFO = "INFO";
	public static final String ERROR = "ERROR";
	
	private static final String CIPHER_SIGNAL = "ciphertext";
	
	public static Map<String, String> parseMapStr(String s) {
		Map<String, String> m = new HashMap<String, String>();
		parseMapStr(m, s);
		return m;
	}

	public static void parseMapStr(Map<String, String> m, String s) {
		String[] ss = s.split(";");
		for (String t : ss) {
			int i = t.indexOf("=");
			if (i > 0) {
				m.put(t.substring(0, i).trim(), t.substring(i + 1).trim());
			}
		}
	}

	public static StringBuilder quoteSafeJson(StringBuilder sb, String string) {
		return sb.append("\"").append(string).append("\"");
	}

	public static String quote(String string) {

		if (string == null || string.length() == 0) 
			return "\"\"";

		char b;
		char c = 0;
		int i;
		int len = string.length();
		StringBuffer sb = new StringBuffer(len * 2);
		String t;
		char[] chars = string.toCharArray();
		char[] buffer = new char[1030];
		int bufferIndex = 0;
		sb.append('"');
		for (i = 0; i < len; i += 1) {
			if (bufferIndex > 1024) {
				sb.append(buffer, 0, bufferIndex);
				bufferIndex = 0;
			}
			b = c;
			c = chars[i];
			switch (c) {
			case '\\':
			case '"':
				buffer[bufferIndex++] = '\\';
				buffer[bufferIndex++] = c;
				break;
			case '/':
				if (b == '<') {
					buffer[bufferIndex++] = '\\';
				}
				buffer[bufferIndex++] = c;
				break;
			default:
				if (c < ' ') {
					switch (c) {
					case '\b':
						buffer[bufferIndex++] = '\\';
						buffer[bufferIndex++] = 'b';
						break;
					case '\t':
						buffer[bufferIndex++] = '\\';
						buffer[bufferIndex++] = 't';
						break;
					case '\n':
						buffer[bufferIndex++] = '\\';
						buffer[bufferIndex++] = 'n';
						break;
					case '\f':
						buffer[bufferIndex++] = '\\';
						buffer[bufferIndex++] = 'f';
						break;
					case '\r':
						buffer[bufferIndex++] = '\\';
						buffer[bufferIndex++] = 'r';
						break;
					default:
						t = "000" + Integer.toHexString(c);
						int tLength = t.length();
						buffer[bufferIndex++] = '\\';
						buffer[bufferIndex++] = 'u';
						buffer[bufferIndex++] = t.charAt(tLength - 4);
						buffer[bufferIndex++] = t.charAt(tLength - 3);
						buffer[bufferIndex++] = t.charAt(tLength - 2);
						buffer[bufferIndex++] = t.charAt(tLength - 1);
					}
				} else {
					buffer[bufferIndex++] = c;
				}
			}
		}
		sb.append(buffer, 0, bufferIndex);
		sb.append('"');
		return sb.toString();
	}

	public static StringBuilder quoteJson(StringBuilder sb, String s){
		return sb.append(quote(s));
	}

	public static final String throwableToString(Throwable t) {
		if (t instanceof FelucaException) {
			return t.getMessage();
		}
		ByteArrayOutputStream ba = new ByteArrayOutputStream();
		PrintWriter p = new PrintWriter(ba);
		t.printStackTrace(p);
		p.flush();
		return ba.toString();
	}

	public static void runShell(String cmd, String pwd, StringBuilder out) {
	}

	/**
	 * Converts some important chars (int) to the corresponding html string
	 */
	static String conv2Html(int i) {
		if (i == '&')
			return "&amp;";
		else if (i == '<')
			return "&lt;";
		else if (i == '>')
			return "&gt;";
		else if (i == '"')
			return "&quot;";
		else
			return "" + (char) i;
	}

	public final static void exec(String command, String dir, StringBuilder ret) {
		final String[] COMMAND_INTERPRETER = { "/bin/sh", "-c" };
		final long MAX_PROCESS_RUNNING_TIME = 30 * 1000; // 30 seconds

		String[] comm = new String[3];
		comm[0] = COMMAND_INTERPRETER[0];
		comm[1] = COMMAND_INTERPRETER[1];
		comm[2] = command;
		long start = System.currentTimeMillis();
		try {
			// Start process
			Process ls_proc = Runtime.getRuntime().exec(comm, null,
					new File(dir));
			// Get input and error streams
			BufferedInputStream ls_in = new BufferedInputStream(ls_proc
					.getInputStream());
			BufferedInputStream ls_err = new BufferedInputStream(ls_proc
					.getErrorStream());
			boolean end = false;
			while (!end) {
				int c = 0;
				while ((ls_err.available() > 0) && (++c <= 1000)) {
					ret.append(conv2Html(ls_err.read()));
				}
				c = 0;
				while ((ls_in.available() > 0) && (++c <= 1000)) {
					ret.append(conv2Html(ls_in.read()));
				}
				try {
					ls_proc.exitValue();
					// if the process has not finished, an exception is thrown
					// else
					while (ls_err.available() > 0)
						ret.append(conv2Html(ls_err.read()));
					while (ls_in.available() > 0)
						ret.append(conv2Html(ls_in.read()));
					end = true;
				} catch (IllegalThreadStateException ex) {
					// Process is running
				}
				// The process is not allowed to run longer than given time.
				if (System.currentTimeMillis() - start > MAX_PROCESS_RUNNING_TIME) {
					ls_proc.destroy();
					end = true;
					ret.append("!!!! Process has timed out, destroyed !!!!!");
				}
				try {
					Thread.sleep(50);
				} catch (InterruptedException ie) {
			 
				}
			}
			
			ls_err.close();
			ls_in.close();
		} catch (IOException e) {
			ret.append("Error: " + e);
		}
	}

	public static final void arrayToString(StringBuilder sb, String name,
			Object[] os) {
		sb.append(name).append(":");
		if (os == null) {
			sb.append(" null \n");
			return;
		}
		sb.append("[");
		for (Object o : os) {
			sb.append(o).append(",");
		}
		sb.append("]\n");
	}
	
	public static final void arrayToAppend(StringBuilder sb, String name,
			Object[] os) {
		sb.append( String.format("\"%s\"", name)).append(":");
		if (os == null) {
			sb.append(" null \n");
			return;
		}
		if (os.length == 0){
			sb.append("[]");
			return;
		}
		sb.append("[");
		for (int i = 0 ; i < os.length-1; i++) {
			sb.append(os[i]).append(",");
		}
		
		sb.append(os[os.length-1] + "]\n");
	}
	
	
	
	public static String extractCollectionName(String fullName){
		int ii = fullName.lastIndexOf("_");
		if (ii == -1)
			return fullName;
		return fullName.substring(0, ii);
	}
	
	public static List<String> split(String sequence, String regex){
		String[] tmp = sequence.split(regex);
		List<String> result = new ArrayList<String>(tmp.length);
		for(String t : tmp){
			result.add(t);
		}
		return result;
	}
	
	
	@SuppressWarnings("rawtypes")
	public static String display(Collection iterable){
		if (iterable == null){
			return null;
		}
		if (iterable.isEmpty()){
			return "[]";
		}
		StringBuilder builder = new StringBuilder("[");
		for(Object obj : iterable){
			if (obj instanceof String){
				builder.append("\"" + obj + "\", ");
			}else{
				builder.append( obj + ", ");
			}
		}
		return builder.substring(0, builder.length() - 2) +"]";
	}
	
	/**
	 * require keyValues.length % 2 == 0, keyValues[i] is key, keyValues[i+1] is value!
	 * @param keyValues
	 * @return
	 */
	public static String keyValuesToJsonString(Object... keyValues){
		return keyValuesToJson(keyValues).toJSONString();
	}
	
	/**
	 * require keyValues.length % 2 == 0, keyValues[i] is key, keyValues[i+1] is value!
	 * @param keyValues
	 * @return
	 */
	private static JSONObject keyValuesToJson(Object... keyValues){
		JSONObject json = new JSONObject();
		for(int i = 0 ; i < keyValues.length; i+=2){
			json.put(keyValues[i].toString(), keyValues[i+1]);
		}
		return json;
	}
	
	
	public static String kvNetworkMsgFormat(Object... keyValues){
		JSONObject bag = keyValuesToJson(keyValues);
		bag.put(CIPHER_SIGNAL, CipherUtil.generatePassword(Constants.Network.leaderToWorkerText));
		return bag.toJSONString();
	}
	
	
	public static boolean isNetworkMsg(String message){
		JSONObject json = JSONObject.parseObject(message);
		String cipherText = json.getString(CIPHER_SIGNAL);
		if (StringUtils.isEmpty(cipherText)){
			return false;
		}
		return CipherUtil.validatePassword(cipherText, Constants.Network.leaderToWorkerText);
	}
}
