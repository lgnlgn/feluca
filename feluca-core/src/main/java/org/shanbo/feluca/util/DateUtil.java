package org.shanbo.feluca.util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

public class DateUtil {
	public static long getMsDateTimeFormat(){
		DateTime dt = new DateTime();
		String dtString = dt.toString(DateTimeFormat.forPattern("yyyyMMddHHmmssSSS"));
		return Long.parseLong(dtString);
	}
	
	
	
}
