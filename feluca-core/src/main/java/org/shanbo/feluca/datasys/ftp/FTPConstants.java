package org.shanbo.feluca.datasys.ftp;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FTPConstants {
	
	public static final String LOGIN_USERNAME = "admin";
	public static final String LOGIN_PASSWORD = "admin";
	
	public static final int PORT = 12221;
	
	static Logger log = LoggerFactory.getLogger(FTPConstants.class);
	
	/**
	 *<b>default data path : ./data</b>
	 *<p> ftpserver.user.admin.userpassword=21232F297A57A5A743894A0E4A801FC3</p>
	<p>	ftpserver.user.admin.enableflag=true
	<p>ftpserver.user.admin.writepermission=true
	<p>ftpserver.user.admin.maxloginnumber=0
<p>ftpserver.user.admin.maxloginperip=0
<p>ftpserver.user.admin.idletime=0
<p>ftpserver.user.admin.uploadrate=0
<p>ftpserver.user.admin.downloadrate=0
	 * @return
	 */
	public static Properties generateDefaultFtpProperties(){
		return generateDefaultFtpProperties("./data");
	}

	/**
	 *<b>specify the data path </b>
	 *<p> ftpserver.user.admin.userpassword=21232F297A57A5A743894A0E4A801FC3</p>
		<p>ftpserver.user.admin.homedirectory=${YOUR_PATH}
	<p>	ftpserver.user.admin.enableflag=true
	<p>ftpserver.user.admin.writepermission=true
	<p>ftpserver.user.admin.maxloginnumber=0
<p>ftpserver.user.admin.maxloginperip=0
<p>ftpserver.user.admin.idletime=0
<p>ftpserver.user.admin.uploadrate=0
<p>ftpserver.user.admin.downloadrate=0
	 * @return
	 */
	public static Properties generateDefaultFtpProperties(String dataDir){
		Properties p = new Properties();
		InputStream in = FTPConstants.class.getResourceAsStream("users.properties");
		try {
			p.load(in);
			p.setProperty("ftpserver.user.admin.homedirectory", dataDir);
			in.close();
		} catch (IOException e) {
			log.warn("load default properties error~~~~~~",e);
		}

		return p;
	}
	
	
	
}
