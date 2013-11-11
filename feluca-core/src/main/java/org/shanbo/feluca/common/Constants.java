package org.shanbo.feluca.common;


public class Constants {
	public final static String DATA_DIR = "./data";
	public final static String INDEX_DIR = DATA_DIR + "/indexes"; 
	public final static String ENDL = System.getProperty("line.separator");
	
	public final static String JSON_CONTENTS_TYPE = "application/json; charset=UTF-8";
	
	public final static String SEARCH_CHROOT = Config.get().get("search.chroot", "/search");
	
	public final static String CSEARCH_PATH = SEARCH_CHROOT + "/csearch";

	
	public static final String REALCOLLECTION_KEY = "indexName";
	public static final String JSON_CALLBACK = "callback";
	public static final String CONFIG_NAME = "index.config";
	
	public static final String SOLR_CHROOT = "/solr";
	
	public static final String ABTEST_RATIO = "ratio";
	public static final String ABTEST_STRING = "abtest";
	public static final String ABTEST_PARAM = "param";
	public static final String ABTEST_ABBR = "simple";
	
	public final static String SELECT_CLAUSE = "select";

	public final static String STRING_FIELDS="fields";
	public final static String DATE_FIELDS="datefields";
	public final static String MULTI_FIELDS="propfields";
	public final static String CONVERT_FIELDS="convertfields";
	public final static String LSTUPD = "lstupd";
	
	public final static String DELETE_CLAUSE = "delete";
	
}