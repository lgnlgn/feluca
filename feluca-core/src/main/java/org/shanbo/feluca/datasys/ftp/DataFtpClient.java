package org.shanbo.feluca.datasys.ftp;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.shanbo.feluca.datasys.DataClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 *  @Description: TODO
 *	@author shanbo.liang
 */
public class DataFtpClient implements DataClient{
	static Logger log = LoggerFactory.getLogger(DataFtpClient.class);
	FTPClient ftpClient;
	int port = FTPConstants.PORT;

	/**
	 * client opened on construction 
	 * @param ip
	 * @throws SocketException
	 * @throws IOException
	 */
	public DataFtpClient(String ip) throws SocketException, IOException{
		ftpClient = new FTPClient(); 
		//		ftpClient.setControlEncoding("GBK"); 
		ftpClient.setDefaultPort(port); 
		ftpClient.connect(ip); 
		ftpClient.login(FTPConstants.LOGIN_USERNAME, FTPConstants.LOGIN_PASSWORD); 
		int reply = ftpClient.getReplyCode(); 
		ftpClient.setDataTimeout(120000); 

		if (!FTPReply.isPositiveCompletion(reply)) { 
			ftpClient.disconnect(); 
			log.error("FTP server refused connection."); 
		}

	}

	public boolean makeDirecotry(String dataName) throws IOException {
		return ftpClient.makeDirectory(dataName);
	}

	public boolean removeDirecotry(String dataName) throws IOException {

		FTPFile[] listFiles = ftpClient.listFiles(dataName);
		for(FTPFile ftpfile : listFiles){
			if (ftpfile.isFile()){
				ftpClient.deleteFile(dataName + "/" + ftpfile.getName());
			}else if (ftpfile.isDirectory()){
				removeDirecotry(dataName + "/" + ftpfile.getName());
				ftpClient.removeDirectory(dataName);
			}
		}
		return ftpClient.removeDirectory(dataName); 

	}

	public boolean copyToRemote(String dataName, File file) throws IOException {
		boolean flag = false;
		try{
			ftpClient.enterLocalPassiveMode(); 
			ftpClient.setFileTransferMode(FTP.STREAM_TRANSFER_MODE); 
			InputStream input = new FileInputStream(file); 
			input = new BufferedInputStream(input);
			flag = ftpClient.storeFile(dataName + "/" + file.getName(), input); 
			if (flag) { 
				System.out.println("上传文件成功！"); 
			} else { 
				System.out.println("上传文件失败！"); 
			} 
			input.close(); 
			flag = true;
		} catch (Exception e) { 
			log.error("aa",e);
			throw new IOException("????");
		} 
		return flag; 
	}

	public List<String> listData() throws IOException {
		List<String> dataNames = new ArrayList<String>();

		String files[] = ftpClient.listNames("."); 
		if (files == null || files.length == 0) 
			;
		else { 
			for (int i = 0; i < files.length; i++) { 
				dataNames.add(files[i]);
			} 
		} 

		return dataNames;
	}

	public String showDataInfo(String dataName) throws IOException {
		
		return null;
	}

	
	public boolean downFromRemote(String dataName) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	public void close() {
		try {
			ftpClient.logout();
			ftpClient.disconnect(); 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

	}

}
