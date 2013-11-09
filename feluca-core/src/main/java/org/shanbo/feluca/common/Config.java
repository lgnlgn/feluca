package org.shanbo.feluca.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * global configuration
 * @author lgn-mop
 *
 */
public abstract class Config {
	static Logger log = LoggerFactory.getLogger(Config.class);
	
	static String configpath = "./conf/config.properties";
	
	
	private static final Config CONFIG = new Config() {
		java.util.Properties p = new Properties();
		boolean changed = false;
		// java.util.Properties ptoStore = null;
		private final File cf = new File( configpath );
		long lmd;

		{
			try {
				if (!cf.exists()) {
					if( !cf.getParentFile().mkdirs() )
						log.warn("Parent file exist or create wron");
					
					if( !cf.createNewFile() )
						log.warn("File exist or create wron");
				}
				
				// ptoStore = new Properties();
				java.lang.Runtime.getRuntime().addShutdownHook(
						new Thread("store-config") {
							public void run() {
								if (changed) {
									boolean autoUpdate = p.containsKey("autoUpdate");
									if (autoUpdate) {
										FileOutputStream fos = null;
										try {
											fos = new java.io.FileOutputStream(cf);
											p.store(fos,
												"add an <autoUpdate> key to auto update config form default values");
										} catch (FileNotFoundException e) {
											log.warn("FileNotFoundException", e);
										} catch (IOException e) {
											log.warn("IOException", e);
										} finally{
											if( fos != null )
												try {
													fos.close();
												} catch (IOException e) {
													log.warn("Close FileOuputStream", e);
												}
										}
									}
								}	
							}
						});

				FileInputStream inputStream = null;
				try {
					inputStream = new java.io.FileInputStream(cf);
					p.load(inputStream);
				} catch (Exception e) {
					log.error("Error while loading config file:{}",
							cf.getAbsolutePath());
				} finally {
					if( inputStream != null )
						inputStream.close();
				}
				
				log.info("loading config from:" + cf.getAbsolutePath());

				lmd = cf.lastModified();

				Thread t = new Thread(new Runnable() {
					public void run() {

						try {
							Thread.sleep(60000);
						} catch (InterruptedException e) {
						}
						long newlmd = cf.lastModified();
						if (newlmd > lmd) {
							lmd = newlmd;
							log.info("Config file {} is changed,reloading ...",
									cf.getAbsolutePath());
							
							FileInputStream inputStream = null;
							try {
								inputStream = new java.io.FileInputStream(cf);
								p.load( inputStream );
							} catch (IOException e) {
								log.error("Error while loading config file:{}",
										cf.getAbsolutePath());
							} finally {
								if( inputStream != null )
									try {
										inputStream.close();
									} catch (IOException e) {
										log.warn("Close FileOuputStream", e);
									}
							}
						}
					}
				}, "Config file refresher");
				t.setDaemon(true);
				t.start();

			} catch (IOException ex) {
				log.warn("cannot create log file", ex);
			} finally {
				
			}
		}

		public String get(String k, String defaultValue) {
			String s = p.getProperty(k);
			if (s == null) {

				p.setProperty(k, defaultValue);
				changed = true;

				return defaultValue;
			}

			return s;
		}

		public int getInt(String k, int defaultValue) {
			String s = this.get(k, defaultValue + "");

			try {
				return Integer.parseInt(s);
			} catch (Exception e) {
				return defaultValue;
			}

		}

		public boolean getBoolean(String k, boolean defaultValue) {
			String s = this.get(k, defaultValue + "");
			try {
				return Boolean.parseBoolean(s);
			} catch (Exception e) {
				return defaultValue;
			}
		}

		public boolean setProperty(String key, String value) {
			p.setProperty(key, value);
			FileOutputStream fos = null;
			try {
				fos = new java.io.FileOutputStream(cf);
				p.store(fos, "");
				
				return true;
			} catch (Exception ex) {
				log.warn("store config", ex);
				return false;
			} finally {
				try {
					if (fos!= null)
						fos.close();
				} catch (IOException e) {
					log.warn("Close FileOuputStream", e);
				}
			}
		}

		public String get(String key) {
			return p.getProperty(key);
		}
	};

	private Config() {

	}

	abstract public String get(String k, String defaultValue);

	// abstract public StringPair[] getByPrefix(String prefix);

	abstract public int getInt(String k, int defaultValue);

	abstract public boolean getBoolean(String k, boolean defaultValue);

	// added by junsen_ye 20100929 
	abstract public boolean setProperty(String key, String value);

	abstract public String get(String key);

	// end added

	public static final Config get() {
		return CONFIG;

	}
}
