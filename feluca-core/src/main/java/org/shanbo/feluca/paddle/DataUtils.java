package org.shanbo.feluca.paddle;

import java.io.IOException;
import java.util.Properties;

import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.util.FileUtil;

public class DataUtils {
	public static Properties loadForWorker(String dataName) throws IOException{
		return FileUtil.loadProperties(Constants.Base.getWorkerRepository() + "/data/" + 
				 dataName + "/" + dataName + ".sta");
	}
}
