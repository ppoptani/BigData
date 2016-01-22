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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DataPreprocessing extends Configured implements Tool {

	private static HashSet<String> dataList;

	static {
		dataList = new HashSet<String>();
	}

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			try {
				String tempString = value.toString();
				String[] columnData = tempString.split(",");

				Text emitKey = new Text();
				String pattern = "M/d/yy";
				DateFormat sdf = new SimpleDateFormat(pattern);
				Calendar cal = Calendar.getInstance(Locale.US);
				Date date = new Date(columnData[1]);
				dataList.add(columnData[1]);
				sdf.parse(columnData[1]);
				cal.setTime(date);
				int week = cal.get(Calendar.WEEK_OF_YEAR);
				int year = cal.get(Calendar.YEAR);
				String emitKeyString = String.valueOf(week) + "," + String.valueOf(year) + ",";
				emitKey.set(columnData[0]+","+emitKeyString);
				context.write(emitKey, value);

			}

			catch (ParseException pe) {
				pe.getStackTrace();
			}

		}

	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		private String stockSymbol = "";

		@Override
		public void reduce(Text key, Iterable<Text> stockHistoryForWeek, Context context)
				throws IOException, InterruptedException {
			double[] avgValues = new double[6];

			for (Text day : stockHistoryForWeek) {
				double[] stockPrices = getStockPrice(day.toString().split(","));
				for (int i = 0; i < avgValues.length; i++) {
					avgValues[i] = avgValues[i] + stockPrices[i];

				}

			}

			for (int i = 0; i < avgValues.length; i++) {
				avgValues[i] = avgValues[i] / 5;

			}

			context.write(key, parse(avgValues));
		}

		private Text parse(double[] avg) {
			StringBuilder builder = new StringBuilder();
			//builder.append(stockSymbol + ",");
			for (double d : avg) {
				builder.append(String.format("%.3f", d));
				builder.append(',');
			}

			Text lastDay = new Text();
			lastDay.set(builder.toString());
			return lastDay;
		}

		private double[] getStockPrice(String[] week) {
			double[] stocks = new double[week.length - 2];
			stockSymbol = week[0];
			for (int i = 0; i < stocks.length; i++) {
				stocks[i] = Double.parseDouble(week[i + 2]);
			}
			return stocks;
		}
	}

	public static class DateGeneratorMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			try {

				String tempString = value.toString();
				String[] columnData = tempString.split(",", 4);

				Text emitKey = new Text();
				String pattern = "M/d/yy";
				int weekYear = Integer.parseInt(columnData[2]);
				int weekOfYear = Integer.parseInt(columnData[1]);
				DateTimeFormatter dtfOut = DateTimeFormat.forPattern(pattern);

				for (int i = 1; i < 8; i++) {
					DateTime dt = new DateTime().withWeekOfWeekyear(weekOfYear).withYear(weekYear).withDayOfWeek(i);

					String emitKeyString = dtfOut.print(dt);
					if (!dataList.contains(emitKeyString)) {
						emitKey.set(columnData[0] + "," + emitKeyString + ",");
						context.write(emitKey, new Text(columnData[3].trim()));

					}

				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	public static class DateOriginator extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String tempString = value.toString();
			String[] columnData = tempString.split(",", 3);
			Text t = new Text(String.valueOf(columnData[0] + "," + columnData[1]) + ",");
			context.write(t, new Text(columnData[2]));
		}

	}

	public static class DateMainReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> stockHistoryForWeek, Context context)
				throws IOException, InterruptedException {

			String line = null;
			double[] stockPrices = new double[6];
			for (Text day : stockHistoryForWeek) {
				stockPrices = getStockPrice(day.toString().split(","));

			}
			context.write(key, parse(stockPrices));

		}

		private static double[] getStockPrice(String[] week) {
			double[] stocks = new double[week.length];
			for (int i = 0; i < stocks.length; i++) {
				stocks[i] = Double.parseDouble(week[i]);
			}
			return stocks;
		}

		private static Text parse(double[] avg) {
			StringBuilder builder = new StringBuilder();
			for (double d : avg) {
				builder.append(String.format("%.3f", d));
				builder.append(',');
			}

			Text lastDay = new Text();
			lastDay.set(builder.toString().trim());
			return lastDay;
		}

	}

	public static void main(String args[]) throws Exception {
		DataPreprocessing datePreProcessing = new DataPreprocessing();
		int res = ToolRunner.run(new Configuration(), datePreProcessing, args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {		
		File[] files = new File(args[0]).listFiles();

		for (File file : files) {

			if (file.getName().equalsIgnoreCase(".DS_Store"))
				continue;
			Configuration conf = this.getConf();
			Job job = Job.getInstance(conf, "date pre processing");
			job.setJarByClass(DataPreprocessing.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			File file1 = new File(args[1]+"/"+file.getName());
			File file2 = new File(args[2]+"/"+file.getName().replaceFirst(".csv", ""));
			Path inputFilePath = new Path(args[0]);
			Path midFilePath = new Path(file1.getPath());
			Path outputFilePath = new Path("finalOutput");

			/* This line is to accept the input recursively */
			FileInputFormat.setInputDirRecursive(job, true);
			FileSystem fs = FileSystem.newInstance(conf);

			if (fs.exists(midFilePath)) {
				fs.delete(midFilePath, true);
			}

			if (fs.exists(outputFilePath)) {
				fs.delete(outputFilePath, true);
			}

			FileInputFormat.addInputPath(job, inputFilePath);
			FileOutputFormat.setOutputPath(job, midFilePath);
			job.waitForCompletion(true);

			Job job1 = Job.getInstance(conf, "date main generator");
			job1.setJarByClass(DataPreprocessing.class);
			job1.setMapperClass(DateGeneratorMapper.class);
			job1.setReducerClass(DateMainReducer.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);

			MultipleInputs.addInputPath(job1, inputFilePath, TextInputFormat.class, DateOriginator.class);
			MultipleInputs.addInputPath(job1, midFilePath, TextInputFormat.class, DateGeneratorMapper.class);
			FileOutputFormat.setOutputPath(job1, outputFilePath);

			job1.waitForCompletion(true);

		}

		return 1;
	}
}