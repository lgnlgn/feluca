package org.shanbo.feluca.datasys.ftp;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
	String ip;
	int port = FTPConstants.PORT;

	/**
	 * client opened on construction 
	 * @param ip
	 * @throws SocketException
	 * @throws IOException
	 */
	public DataFtpClient(String ip) throws SocketException, IOException{
		this.ip = ip;
		ftpClient = new FTPClient(); 
		//		ftpClient.setControlEncoding("GBK"); 
		ftpClient.setDefaultPort(port); 
		ftpClient.connect(ip); 
		ftpClient.login(FTPConstants.LOGIN_USERNAME, FTPConstants.LOGIN_PASSWORD); 
		int reply = ftpClient.getReplyCode(); 
		ftpClient.setDataTimeout(120000); 
		ftpClient.enterLocalPassiveMode();
		ftpClient.setFileType(FTP.BINARY_FILE_TYPE); 
		ftpClient.setBufferSize(64*1024);
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

	public boolean copyToRemote(String dataName, File toCopy) throws IOException {
		boolean flag = false;
		try{

			InputStream input = new FileInputStream(toCopy); 
			input = new BufferedInputStream(input);
			flag = ftpClient.storeFile(dataName + "/" + toCopy.getName(), input); 
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


	public boolean downFromRemote(String remoteFileName, String localDires) throws IOException {

		String strFilePath = localDires + "/" + remoteFileName;
		File localFile = new File(localDires);
		if(!localFile.exists() || !localFile.isDirectory())
			localFile.mkdirs();
		BufferedOutputStream outStream = null;  
		boolean success = false;  
		try {  
			//	            ftpClient.changeWorkingDirectory(remoteDownLoadPath);  
			outStream = new BufferedOutputStream(new FileOutputStream(  strFilePath));  
			success = ftpClient.retrieveFile(remoteFileName, outStream);  
			if (success == true) {  
				return success;  
			}  
		} catch (IOException e) {  
			log.error("down load file" + remoteFileName + " error" ,e);
		} finally {  
			if (null != outStream) {  
				try {  
					outStream.flush();  
					outStream.close();  
				} catch (IOException e) {  
					e.printStackTrace();  
				}  
			}  
		}  
		return success;  

	}

	public void close() {
		try {
			ftpClient.logout();
			ftpClient.disconnect(); 
		} catch (IOException e) {
			e.printStackTrace();
		} 

	}

	public static void main(String[] args) throws SocketException, IOException {
		DataFtpClient client = new DataFtpClient("10.249.9.205");
		System.out.println("----");
		long t = System.currentTimeMillis();
		client.copyToRemote("aaa", new File("data/aaa/001.pdf"));
		System.out.println("---" + (System.currentTimeMillis()-t));
		client.close();
	}
}
