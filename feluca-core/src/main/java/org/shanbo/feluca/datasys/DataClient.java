package org.shanbo.feluca.datasys;

import java.io.File;
import java.util.List;

public interface DataClient {
	public boolean mkdir(String path);
	
	public boolean copyToRemote(File... files);
	
	public List<String> listData();
	
	public String showDataInfo(String dataName);
	
	public boolean downFromRemote(String dataName);
	
}
