package com.pcwk.ehr.cmn;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DateCmn {

	static public String str_To_Week(String str_date) {

		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

		LocalDateTime dateTime = LocalDateTime.parse(str_date, dtf);

		String week = dateTime.getDayOfWeek().name();

		if (week.equals("MONDAY")) {
			return "월";
		} else if (week.equals("TUESDAY")) {
			return "화";
		} else if (week.equals("WEDNESDAY")) {
			return "수";
		} else if (week.equals("THURSDAY")) {
			return "목";
		} else if (week.equals("FRIDAY")) {
			return "금";
		} else if (week.equals("SATURDAY")) {
			return "토";
		} else {
			return "일";
		}

	}
	
	static public String now_Str(String hour_min) {
		
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd " + hour_min);
		
		String now_Str= LocalDateTime.now().format(dtf);
		
		return now_Str;	
		
		
		
	}
	
	

}
