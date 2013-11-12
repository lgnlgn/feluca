/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shanbo.feluca.common.http;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.shanbo.feluca.common.Strings;


/**
 * 
 * @author lgn
 *
 */
public class Handlers {

	Map<String, Handler> handlers =  new java.util.concurrent.ConcurrentHashMap<String, Handler>();
	
	
	public void addHandler(Handler... h) {
		for (Handler a : h) {
			handlers.put(a.getPath(), a);
		}
	}

	
	public Handler getHandler(String path) {
		return  handlers.get(path);
	}
	
	public Set<String> getHandlerPaths(){
		Set<String> resultList = new HashSet<String>();
		resultList.addAll(this.handlers.keySet());
		return resultList;
	}
	

	public String toString(){
		StringBuilder sb = new StringBuilder("{");
		String[] handlerNames = new String[this.handlers.size()];
		int i = 0;
		for(Handler handler : handlers.values()){
			handlerNames[i++] = "\"" + handler.getClass().getName() + "\"";
		}
		Strings.arrayToAppend(sb, "response", handlerNames);
		sb.append("}");
		return sb.toString();
	}
}
