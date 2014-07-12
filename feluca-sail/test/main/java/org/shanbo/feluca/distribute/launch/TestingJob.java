package org.shanbo.feluca.distribute.launch;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.shanbo.feluca.data.Vector;
import org.shanbo.feluca.util.AlgoDeployConf;
import org.shanbo.feluca.util.FileUtil;
import org.shanbo.feluca.util.JSONUtil;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableList;

public class TestingJob {

	public static class SleepJob extends LoopingBase{
		Random r = new Random();
		public SleepJob(GlobalConfig conf) throws Exception {
			super(conf);
		}

		@Override
		public void createVectorDB() throws InterruptedException, ExecutionException {
			vectorClient.createVector("test", 100, -1f);
			
		}

		@Override
		public void dumpVectorDB() throws InterruptedException, ExecutionException {
			vectorClient.dumpVector("test", "");
		}

		@Override
		public boolean earlyStop() {
			return false;
		}

		@Override
		protected void compute() {
			long[] offsetArray = dataReader.getOffsetArray(); 

			for(int o = 0 ; o < offsetArray.length; o++){ 
				Vector v = dataReader.getVectorByOffset(offsetArray[o]);
			}
			try {
				Thread.sleep(r.nextInt(3000) + 3000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		GlobalConfig globalConfig = GlobalConfig.build("sleep", JSONUtil.basicAlgoConf(2),
				"covtype", FileUtil.loadProperties("data/covtype/covtype.sta"),
				ImmutableList.of("192.168.1.100:12030"), ImmutableList.of("192.168.1.100:12130"), 
				"192.168.1.100:12030", new AlgoDeployConf(true, true, true, true));
		SleepJob sj = new SleepJob(globalConfig);
		sj.run();
	}

}
