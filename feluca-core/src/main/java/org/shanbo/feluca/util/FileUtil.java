package org.shanbo.feluca.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;


public class FileUtil {
	/**
	 * 遍历文件夹删除所有文件
	 * @param file
	 */
	public synchronized static void deleteFile(File file){ 
		if(file.exists()){ 
			if(file.isFile()){ 
				file.delete(); 
			}else if(file.isDirectory()){ 
				File files[] = file.listFiles(); 
				for(int i=0;i<files.length;i++){ 
					deleteFile(files[i]); 
				} 
			} 
			file.delete(); 
		}else{ 
			System.out.println("所删除的文件不存在！"); 
		} 
	} 
	
	/**
	 * @param testopen (testopen dirname must have the same form to already-exists one）
	 * @return dir status, 0 for not found old index dir, 1 for ok, -1 for mkdir failure
	 * @throws IOException
	 */
	public synchronized static int makeIndexDir(File testopen, String parentdir) throws IOException{
		//testopen dirname must have the same form to already-exists one
		File olddir = null;
		for(File dir: new File(parentdir).listFiles()){
			if(	dir.getName().replaceAll("\\.\\d+", "").equals(
					testopen.getName().replaceAll("\\.\\d+", ""))){
				olddir = new File(dir.getAbsolutePath());
				break;//only one dirname match
			}
		}
		
		if (olddir ==null)
			return 0;
		else{
			try{
				String[] dirdetail =  olddir.getName().split("\\.");
				if (dirdetail.length<2)
					return 0;
				return Integer.parseInt(dirdetail[1])+1;
				
			}catch(Exception e){			
				return -1;
			}
		}			
	}
	

	/** 
	 * 复制整个文件夹内容,来自互联网  
	 *   
	 * @param srcPath  
	 *            String 原文件路径 如：c:/old
	 * @param newPath  
	 *            String 复制后路径 如：f:/new
	 **/ 
	public synchronized static void copyFolder(String srcPath, String newPath) {   
	  
	    try {   
	        (new File(newPath)).mkdirs(); // 如果文件夹不存在 则建立新文件夹   
	        File a = new File(srcPath);   
	        String[] file = a.list();   
	        File temp = null;   
	        for (int i = 0; i < file.length; i++) {   
	            if (srcPath.endsWith(File.separator)) {   
	                temp = new File(srcPath + file[i]);   
	            } else {   
	                temp = new File(srcPath + File.separator + file[i]);   
	            }   
	            if (temp.isFile()) {   
	                FileInputStream input = new FileInputStream(temp);   
	                FileOutputStream output = new FileOutputStream(newPath + "/" + (temp.getName()).toString());   
	                byte[] b = new byte[1024 * 5];   
	                int len;   
	                while ((len = input.read(b)) != -1) {   
	                    output.write(b, 0, len);   
	                }   
	                output.flush();   
	                output.close();   
	                input.close();   
	            }   
	            if (temp.isDirectory()) {//    
	                copyFolder(srcPath + "/" + file[i], newPath + "/" + file[i]);   
	            }   
	        }   
	    } catch (Exception e) {   
	        System.out.println(" copy ing Exception");   
	        e.printStackTrace();   
	    }   
	}  
	
	public static boolean isUTF8Encoding(String path) throws IOException{
         InputStream in= new java.io.FileInputStream(path);
         byte[] b = new byte[3];
         in.read(b);
         in.close();
         if (b[0] == -17 && b[1] == -69 && b[2] == -65)
             return true;
         else
             return false;
	}

	/**
	 * from Internet
	 * @param src
	 * @param desDir
	 * @throws IOException
	 */
	public static void unJar(String src, File desDir) throws IOException{
		JarInputStream jarIn = new JarInputStream(new BufferedInputStream(new FileInputStream(src)));
		if(!desDir.exists())desDir.mkdirs();
		byte[] bytes = new byte[1024];

		while(true){
			ZipEntry entry = jarIn.getNextJarEntry();
			if(entry == null)break;

			File desTemp = new File(desDir.getAbsoluteFile() + File.separator + entry.getName());

			if(entry.isDirectory()){    //jar条目是空目录
				if(!desTemp.exists())desTemp.mkdirs();
			//	                log.info("MakeDir: " + entry.getName());
			}else{    //jar条目是文件
				BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(desTemp));
				int len = jarIn.read(bytes, 0, bytes.length);
				while(len != -1){
					out.write(bytes, 0, len);
					len = jarIn.read(bytes, 0, bytes.length);
				}

				out.flush();
				out.close();

				//	                log.info("Copyed: " + entry.getName());
			}
			jarIn.closeEntry();
		}

		//解压Manifest文件
		Manifest manifest = jarIn.getManifest();
		if(manifest != null){
			File manifestFile = new File(desDir.getAbsoluteFile()+File.separator+JarFile.MANIFEST_NAME);
			if(!manifestFile.getParentFile().exists())manifestFile.getParentFile().mkdirs();
			BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(manifestFile));
			manifest.write(out);
			out.close();
		}

		//关闭JarInputStream
		jarIn.close();
	}
	
	public static Properties loadProperties(String filePath) throws IOException{
		FileInputStream fis = new FileInputStream(filePath);
		Properties p = new Properties();
		p.load(fis);
		fis.close();
		return p;
	}
	
}
