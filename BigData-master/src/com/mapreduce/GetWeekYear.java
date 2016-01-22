package com.mapreduce;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class GetWeekYear {
	
	public static void main(String args[])throws IOException,ParseException{
		//DateTime date = new DateTime("2015-01-16T00:08:00.000-05:00");
		
		Date curr = new Date();
		DateFormat sdf = new SimpleDateFormat("dd/MM/yy");
		Calendar cal = Calendar.getInstance(Locale.US);
		cal.setTime(sdf.parse("11/4/92"));
		int nweek = cal.WEEK_OF_YEAR;
		int year = cal.YEAR;
		
		DateTimeFormatter dtfOut = DateTimeFormat.forPattern("MM/dd/yyyy");
		
		DateTime dt =new DateTime().withWeekOfWeekyear(20).withYear(2011).withDayOfWeek(6);
		int weekofyear = dt.getWeekOfWeekyear();
		int weekYear = dt.getYear();
		String emitKeyString = dtfOut.print(dt);
		//int week = date.getWeekOfWeekyear();
		System.out.println(emitKeyString );
		
	}

}
