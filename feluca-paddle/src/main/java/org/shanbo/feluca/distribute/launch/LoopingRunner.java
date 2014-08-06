package org.shanbo.feluca.distribute.launch;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.concurrent.Future;

import org.shanbo.feluca.paddle.GlobalConfig;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

import com.alibaba.fastjson.JSONArray;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class LoopingRunner {
	List<GlobalConfig> configs;
	public LoopingRunner(JSONArray configs){
		this.configs = Lists.newArrayList();
		for(int i = 0 ; i < configs.size(); i++){
			this.configs.add(GlobalConfig.parseJSON(configs.getJSONObject(i).toJSONString()));
		}
	}
	
	public void runTasks(Class<? extends LoopingBase> jobClass) throws Exception{
		
		System.out.println("(" + configs.size() + ")tasks : " + Lists.transform(configs, new Function<GlobalConfig, String>() {
			public String apply(GlobalConfig input) {
				return input.getWorkerName();
			}
		}));
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>");
		List<Future<?>> futures = Lists.newArrayList();
		Constructor<? extends LoopingBase> constructor = jobClass.getConstructor(GlobalConfig.class); 
		for(int i = 0 ; i < configs.size() ; i++){
			Runnable r =  constructor.newInstance(configs.get(i));
			futures.add(ConcurrentExecutor.submit(r));
		}
		for( int i = 0; i < futures.size() ; i++){
			futures.get(i).get();
		}
		System.out.println("<<<<<<<<<<<<<<<<<<<<<<");
	}
	
	
}	
