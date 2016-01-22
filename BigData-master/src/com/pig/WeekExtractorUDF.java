package pig;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class WeekExtractorUDF extends EvalFunc<String>{

	@Override
	public String exec(Tuple input) throws IOException {
		// TODO Auto-generated method stub
		
	try {
		 if (input == null || input.size() == 0)
	     return null;
			
		String dateinput = (String)input.get(0);
		String columnData[] = dateinput.split("/",3);
		Date date = new Date(dateinput);
		String pattern = "M/d/yy";
		Calendar cal = Calendar.getInstance(Locale.US);
		DateFormat sdf = new SimpleDateFormat(pattern);
		sdf.parse(dateinput);
		cal.setTime(date);
		int week = cal.get(Calendar.WEEK_OF_YEAR);
		int year = cal.get(Calendar.YEAR);
		String emitKeyString = String.valueOf(week) + String.valueOf(year);
		
		
		return emitKeyString;
		}
		catch(ParseException e){
			e.printStackTrace();
		}
	return null;
	}
		
	
	

}
