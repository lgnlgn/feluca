package org.shanbo.feluca.core.test;

import com.alibaba.fastjson.JSONObject;

public class Json {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		JSONObject jsonObject = JSONObject.parseObject("{'a':\"2\"}");
		Long integer = jsonObject.getLong("a");
		System.out.println(integer);
	}

}
