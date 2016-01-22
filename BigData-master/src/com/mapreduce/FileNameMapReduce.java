package com.mapreduce;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class FileNameMapReduce {

	static String fileName = "";

	public static class DateGeneratorMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String columnData[] = value.toString().split(",");
			Text writeKey = new Text();
			writeKey.set(fileName + ",");

			if (!value.toString().equalsIgnoreCase("Date,Open,High,Low,Close,Volume,Adj Close"))
				context.write(writeKey,value);
		}

		public static class DateMainReducer extends Reducer<Text, Text, Text, Text> {

			@Override
			public void reduce(Text key, Iterable<Text> stockHistory, Context context)
					throws IOException, InterruptedException {

				String emitKeyString = "";

				try{

					for (Text day : stockHistory) {
						String stockDetails[] = day.toString().split(",", 2);
						
						
						Text t = new Text();
						t.set(key + parseDate(stockDetails[0],"YYYY-MM-DD","M/d/yy")+",");
						context.write(t, new Text(stockDetails[1]));

					}
				}
				catch(ParseException e){
					e.printStackTrace();
				}

				

				
			}
			
			private String parseDate(String date, String inputFormat,String outputFormat) throws ParseException
			{
				String column[] = date.split("-",3);
				if(column[0].length()!=4){
					return date;
				}
				LocalDate dt = new LocalDate(Integer.parseInt(column[0]), Integer.parseInt(column[1]), Integer.parseInt(column[2]));
				LocalDate dt1 = LocalDate.parse(dt.toString(outputFormat), DateTimeFormat.forPattern(outputFormat));
				DateTimeFormatter dtfOut = DateTimeFormat.forPattern(outputFormat);		
				return dtfOut.print(dt1);
				
			}

		}

		public static void main(String[] args) throws Exception {
			
			File[] files = new File(args[0]).listFiles();
			int i = 0;
			
			for (File file:files){
			i++;
			if(file.getName().equalsIgnoreCase(".DS_Store"))continue;	
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "word count");

			job.setJarByClass(DateMainGenerator.class);
			job.setMapperClass(DateGeneratorMapper.class);
			job.setReducerClass(DateMainReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setNumReduceTasks(1);
			fileName = file.getName().replaceFirst(".csv", "");

			
			FileInputFormat.setInputDirRecursive(job, true);
			FileSystem fs = FileSystem.newInstance(conf);
			File file2 = new File(fileName);
			Path inputFilePath = new Path(file.getPath());
			Path outputFilePath = new Path(args[1]+"/"+file2.getPath());
			

			if (fs.exists(outputFilePath)) {
				fs.delete(outputFilePath, true);
			}

			FileInputFormat.addInputPath(job, inputFilePath);
			FileOutputFormat.setOutputPath(job, outputFilePath);
			job.waitForCompletion(true);
		}
			System.exit(1);
	}		
	}
}
