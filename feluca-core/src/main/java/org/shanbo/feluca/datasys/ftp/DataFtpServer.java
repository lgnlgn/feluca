package org.shanbo.feluca.datasys.ftp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.listener.ListenerFactory;
import org.shanbo.feluca.datasys.DataServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 *  @Description data server
 *	@author shanbo.liang
 */
public class DataFtpServer implements DataServer{
	static Logger log = LoggerFactory.getLogger(DataFtpServer.class);
	
	FtpServer server; 
	int port ;
	byte[] propInBytes;
	
	
	public DataFtpServer(int port){
		ByteArrayOutputStream temp = new ByteArrayOutputStream();
		this.port = port;

		Properties p = FTPConstants.generateDefaultFtpProperties();
		if (!p.containsKey("ftpserver.user.admin.userpassword"))
			throw new RuntimeException("properties to bytes[] exception");
		try {
			p.store(temp, "");
			propInBytes = temp.toByteArray();
		} catch (IOException e) {
		}
	}

	public DataFtpServer(){
		this(FTPConstants.PORT);
	}

	public void start() throws FtpException {
		FtpServerFactory serverFactory = new FtpServerFactory();
		ListenerFactory factory = new ListenerFactory();
		// set the port of the listener
		factory.setPort(port);

		// replace the default listener
		PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
		userManagerFactory.setInputSteam(new ByteArrayInputStream(propInBytes));

		serverFactory.addListener("default", factory.createListener());
		// start the server
		serverFactory.setUserManager(userManagerFactory.createUserManager());

		server = serverFactory.createServer();         
		server.start();
	}

	public void close() {
		server.stop();
	}

}
