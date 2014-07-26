package org.shanbo.feluca.paddle;

import org.shanbo.feluca.common.Constants;

import com.alibaba.fastjson.JSONObject;

/**
 * needs an abstraction; 
 * @author lgn
 *
 */
public class DefaultAlgoConf {
	public static JSONObject basicAlgoConf(int loops){
		JSONObject json = new JSONObject();
		json.put(Constants.Algorithm.LOOPS, loops);
		return json;
	}
	
	public static JSONObject basicLRconf(int loops, double alpha, double lambda){
		JSONObject basic = basicAlgoConf(loops);
		basic.put("alpha", alpha);
		basic.put("lambda", lambda);
		return basic;
		
	}
}
