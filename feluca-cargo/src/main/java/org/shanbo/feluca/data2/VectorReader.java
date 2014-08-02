package org.shanbo.feluca.data2;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.shanbo.feluca.data2.Vector.VectorType;

public interface VectorReader extends Closeable{
	public VectorType getVectorType();
	
	public Properties getDataStatistic();
	
	public Vector getNextVector() throws IOException;
	
	public File getDataDir();
}
