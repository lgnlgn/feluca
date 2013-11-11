package org.shanbo.feluca.datasys;

import java.io.File;
import java.io.IOException;
import java.util.List;

public interface DataClient {
	
	public void close() ;
	
	public boolean makeDirecotry(String dataName) throws IOException;
	
	public boolean removeDirecotry(String dataName) throws IOException;
	
	public boolean copyToRemote(String dataName, File file) throws IOException;
	
	public List<String> listData() throws IOException;
	
	public String showDataInfo(String dataName) throws IOException;
	
	public boolean downFromRemote(String dataName) throws IOException;
	
}
