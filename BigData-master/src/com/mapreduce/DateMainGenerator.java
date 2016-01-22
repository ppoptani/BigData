package com.mapreduce;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import java.util.HashSet;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DateMainGenerator {

	private static HashSet<String> dataList;

	static {
		dataList = new HashSet<String>();
	}

	public static class DateGeneratorMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String tempString = value.toString();
			String[] columnData = tempString.split(",",3);

			Text emitKey = new Text();
			String pattern = "mm/dd/yy";
			DateFormat sdf = new SimpleDateFormat(pattern);
			Calendar cal = Calendar.getInstance(Locale.US);

			int weekYear = Integer.parseInt(columnData[1]);
			int weekOfYear = Integer.parseInt(columnData[0]);
			for (int i = 1; i <=7; i++) {
				cal.setWeekDate(weekYear, weekOfYear, i);
				if (!dataList.contains(cal.getTime())) {
					String emitKeyString = String.valueOf(cal.getTime());
					emitKey.set(emitKeyString);
					context.write(emitKey, new Text(columnData[2]));

				}

			}

		}

	}

	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(DateMainGenerator.class);
		job.setMapperClass(DateGeneratorMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		//job.setReducerClass(DateGeneratorReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}