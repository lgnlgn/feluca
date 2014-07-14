package org.shanbo.feluca.distribute.launch;

import gnu.trove.set.hash.TIntHashSet;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.data.Vector;
import org.shanbo.feluca.data.convert.DataStatistic;
import org.shanbo.feluca.data.util.CollectionUtil;
import org.shanbo.feluca.distribute.newmodel.PartialVectorModel;
import org.shanbo.feluca.util.AlgoDeployConf;
import org.shanbo.feluca.util.FileUtil;
import org.shanbo.feluca.util.JSONUtil;
import org.shanbo.feluca.util.NetworkUtils;

import com.google.common.collect.ImmutableList;

public class TestingJob {
	
	
	
	public static class SleepJob extends LoopingBase{
		Random r = new Random();
		
		final static String VECTOR_MODEL_NAME = "test";
		
		int cc = 0;
		public SleepJob(GlobalConfig conf) throws Exception {
			super(conf);
		}

		@Override
		public void startup() throws InterruptedException, ExecutionException {
			vectorClient.createPartialModel(VECTOR_MODEL_NAME);
		}

		@Override
		public void cleanup() throws InterruptedException, ExecutionException {
		}

		@Override
		public boolean earlyStop() {
			if (cc  > loops /2){
				return true;
			}else{
				return false;
			}
		}

		@Override
		protected void compute() throws InterruptedException, ExecutionException {
			long[] offsetArray = dataReader.getOffsetArray(); 
			List<long[]> splitted = CollectionUtil.splitLongs(offsetArray, 1000, false);
			for(long[] segment : splitted){
				TIntHashSet idSet = new TIntHashSet();
				for(long offset : segment){ 
					Vector v = dataReader.getVectorByOffset(offset);
					distinctIds(idSet, v);
				}
				int[] currentFIds = idSet.toArray();
				vectorClient.fetchVector(VECTOR_MODEL_NAME, currentFIds);
				PartialVectorModel partialVector = vectorClient.getVector(VECTOR_MODEL_NAME);
				for(int fid : currentFIds){
					partialVector.set(fid, partialVector.get(fid)-1); //minus 1 per segment
				}
				vectorClient.updateCurrentVector(VECTOR_MODEL_NAME, currentFIds);
				System.out.print("!");
			}
			System.out.println();
			try {
				Thread.sleep(r.nextInt(1000) + 1000);
			} catch (InterruptedException e) {
			}
			cc+=1;
		}

		@Override
		protected void modelStart() throws InterruptedException, ExecutionException {
			vectorClient.createVector(VECTOR_MODEL_NAME, 
					conf.getDataStatistic().getIntValue(DataStatistic.MAX_FEATURE_ID), 0f);
			System.out.println("created ");
		}

		@Override
		protected void modelClose() throws InterruptedException, ExecutionException {
			vectorClient.dumpVector(VECTOR_MODEL_NAME, 
					Constants.Base.getWorkerRepository() + "/model/" + conf.getAlgorithmName());			
		}
		
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		List<String> workers = ImmutableList.of(NetworkUtils.ipv4Host() + ":12030", NetworkUtils.ipv4Host() + ":12031");
		List<String> models = ImmutableList.of(NetworkUtils.ipv4Host() + ":12130", NetworkUtils.ipv4Host() + ":12131");
		String thisWorkerName = workers.get(0);
		AlgoDeployConf thisDeployConf = new AlgoDeployConf(true, true, true, true);
		
		GlobalConfig globalConfig = GlobalConfig.build("sleep", JSONUtil.basicAlgoConf(4),
				"covtype", FileUtil.loadProperties("data/covtype/covtype.sta"),
				workers, models, 
				thisWorkerName, thisDeployConf);
		TestingJob.SleepJob sj = new TestingJob.SleepJob(globalConfig);
		sj.run();
	}

}
