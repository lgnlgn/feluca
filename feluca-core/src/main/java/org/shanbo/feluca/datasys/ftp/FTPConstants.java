package org.shanbo.feluca.datasys.ftp;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.shanbo.feluca.common.FelucaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FTPConstants {

	public static final String LOGIN_USERNAME = "admin";
	public static final String LOGIN_PASSWORD = "admin";

	public static final int PORT = 12221;

	static Logger log = LoggerFactory.getLogger(FTPConstants.class);


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
		String ftpProperty = "ftpserver.user.admin.userpassword=21232F297A57A5A743894A0E4A801FC3\n"+
				"ftpserver.user.admin.homedirectory=./leader_repo\n"+
				"ftpserver.user.admin.enableflag=true\n"+
				"ftpserver.user.admin.writepermission=true\n"+
				"ftpserver.user.admin.maxloginnumber=0\n"+
				"ftpserver.user.admin.maxloginperip=0\n"+
				"ftpserver.user.admin.idletime=0\n"+
				"ftpserver.user.admin.uploadrate=0\n"+
				"ftpserver.user.admin.downloadrate=0";
		InputStream in = new ByteArrayInputStream(ftpProperty.getBytes());

		try {
			p.load(in);
		} catch (IOException e) {
		}
		p.setProperty("ftpserver.user.admin.homedirectory", dataDir);

		return p;
	}



}
