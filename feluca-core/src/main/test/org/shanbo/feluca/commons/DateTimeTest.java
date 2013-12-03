package org.shanbo.feluca.commons;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

public class DateTimeTest {

	public static void main(String[] args) {
		DateTime dt = new DateTime();
		System.out.println(dt.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss SSS")));


	}

}
