package org.shanbo.feluca.node.worker;

import java.io.IOException;
import java.util.Map;

import org.shanbo.feluca.node.RoleModule;

public class WorkerModule extends RoleModule{
	private String dataDir; //
	private volatile Map<String, String> workers;
	
	public static void main(String[] args) throws IOException {
		ProcessBuilder pb = new ProcessBuilder("java", "test1");
		Process p = pb.start();
		
	}
}
