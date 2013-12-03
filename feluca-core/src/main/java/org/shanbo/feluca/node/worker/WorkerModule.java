package org.shanbo.feluca.node.worker;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.time.DateUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.shanbo.feluca.node.RoleModule;

public class WorkerModule extends RoleModule{
	private String dataDir; //
	private volatile Map<String, String> workers;
	
	private WorkerJobManager manager;
	
	
	public List<String> listDataBlocks(String dataName){
		return null;
	}
	
	public String submitTask(Object task){
		return null;
	}
	
	public Object getJobStatus(){
		return null;
	}
	
	
	public Object getTaskLog(String taskName){
		return null;
	}
	
	public static void main(String[] args) throws IOException {
		ProcessBuilder pb = new ProcessBuilder("java", "test1");
		Process p = pb.start();
		
	}
}
