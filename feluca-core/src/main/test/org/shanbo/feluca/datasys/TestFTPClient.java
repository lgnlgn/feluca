package org.shanbo.feluca.datasys;

import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.util.List;

import org.shanbo.feluca.datasys.ftp.DataFtpClient;
import org.shanbo.feluca.datasys.ftp.DataFtpServer;

public class TestFTPClient {

	public static void main(String[] args) throws SocketException, IOException {
		// TODO Auto-generated method stub
		DataFtpClient client = new DataFtpClient("localhost");
		client.makeDirecotry("aaa");
		
//		boolean ok = client.copyToRemote("aaa", new File("g:/devimlw.a2s.index"));
//		System.out.println(ok);
		List<String> listData = client.listData();
		System.out.println(listData);
		client.close();
	}

}
