package org.shanbo.feluca.node.worker.job;

import java.util.List;
import java.util.Properties;

import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.shanbo.feluca.common.FelucaJob;

public class PullDataJob extends FelucaJob{

	public static class Puller extends FelucaJob{

		public String address;
		public String[] files;
		
		public Puller(Properties prop) {
			super(prop);
			files = prop.getProperty("blocks", "").split(",");
		}

		@Override
		protected String getAllLog() {
			return null;
		}
		
	}
	
	public PullDataJob(Properties prop) {
		super(prop);
	}

	@Override
	protected String getAllLog() {

		return null;
	}
	
}
