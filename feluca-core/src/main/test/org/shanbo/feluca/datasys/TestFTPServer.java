package org.shanbo.feluca.datasys;

import org.shanbo.feluca.datasys.ftp.DataFtpServer;

public class TestFTPServer {

	public static void main(String[] args) throws Exception {
		DataServer ds = new DataFtpServer();
		ds.start();
	}

}
