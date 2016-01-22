package pig;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class QuarterComputationPig extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
		
		try{
		if(input==null||input.size()==0)
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
		
		
		return String.valueOf(calculateQuarter(week, year));
		}
		catch (ParseException pe){
			pe.printStackTrace();
		}
		return null;
	}
	
	
	public static int calculateQuarter(int week,int year){
		
		if(week<=13) return 1;
		else if(week>13 && week<=26)return 2;
		else if(week>27 && week<=39)return 3;
		else return 4;
		
	}
}
