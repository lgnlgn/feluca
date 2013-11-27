package org.shanbo.feluca.datasys;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 *  client for manipulating data on remote location
 *  @Description TODO
 *	@author shanbo.liang
 */
public interface DataClient {
	
	public void close() ;
	
	public boolean makeDirecotry(String destDataName) throws IOException;
	
	
	public boolean removeDirecotry(String destDataName) throws IOException;
	
	/**
	 * 
	 * @param dataName
	 * @param file exist data block in datadir/dataName
	 * @return
	 * @throws IOException
	 */
	public boolean copyToRemote(String destDataName, File toCopy) throws IOException;
	
	public List<String> listData() throws IOException;
	
	public String showDataInfo(String dataName) throws IOException;
	
	public boolean downFromRemote(String dataName) throws IOException;
	
}
