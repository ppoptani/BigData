package mahout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
//import org.apache.log4j.spi.LoggerFactory;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.Vector.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.mahout.math.*;

public class SimpleKMeansClustering {

	public static boolean isDouble(String str) {
		try {
			Double.parseDouble(str);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	public static void writePointsToFile(List<Vector> points, String fileName, FileSystem fs, Configuration conf)
			throws IOException {
		Path path = new Path(fileName);
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, LongWritable.class, VectorWritable.class);
		long recNum = 0;
		VectorWritable vec = new VectorWritable();
		for (Vector point : points) {
			vec.set(point);
			writer.append(new LongWritable(recNum++), vec);
		}
		writer.close();
	}

	public static void main(String args[]) throws Exception {

		int k = 3;
		HashMap<Double,String> symbolList = new HashMap<Double,String>();
		
		double counter =1.0;
		
		List<Vector> vectors = new ArrayList<Vector>();
		// The name of the file to open.
		String fileName = "/Users/ukothan/Documents/KMeansData.csv";
		//
		//// This will reference one line at a time
		String line = null;
		//
		try {
			// FileReader reads text files in the default encoding.
			FileReader fileReader = new FileReader(fileName);
			// long i=0;
			// // Always wrap FileReader in BufferedReader.
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			while ((line = bufferedReader.readLine()) != null) {

				StringTokenizer st = new StringTokenizer(line, ",");
				
				symbolList.put(counter++, st.nextToken());
				
				double d[] = new double[] { counter, Double.parseDouble(st.nextToken()) };
				Vector v1 = new RandomAccessSparseVector(d.length);
				v1.assign(d);
				vectors.add(v1);

			}

			// Always close files.
			bufferedReader.close();
		} catch (FileNotFoundException ex) {
			System.out.println("Unable to open file '" + fileName + "'");
		} catch (IOException ex) {
			System.out.println("Error reading file '" + fileName + "'");

		}

		File testData = new File("testdata");
		if (!testData.exists()) {
			testData.mkdir();
		}
		testData = new File("testdata/points");
		if (!testData.exists()) {
			testData.mkdir();
		}

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		writePointsToFile(vectors,"testdata/points/file1",fs,conf);

		
		Path path = new Path("testdata/clusters/part-00000");
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, Text.class, Kluster.class);

		for (int i = 0; i < k; i++) {
			Vector vec = vectors.get(i);
			Kluster cluster = new Kluster(vec, i, new EuclideanDistanceMeasure());
			writer.append(new Text(cluster.getIdentifier()), cluster);
		}
		writer.close();

		Path output = new Path("output");
		HadoopUtil.delete(conf, output);

		KMeansDriver.run(conf, new Path("testdata/points"), new Path("testdata/clusters"), output,
				new EuclideanDistanceMeasure(), 0.001, 10, true, 0.0, false);

		SequenceFile.Reader reader = new SequenceFile.Reader(fs,
				new Path("output/" + Kluster.CLUSTERED_POINTS_DIR + "/part-m-00000"), conf);
		PrintWriter pw = new PrintWriter("outputClusters.txt");
		IntWritable key = new IntWritable();
		WeightedVectorWritable value = new WeightedVectorWritable();
		while (reader.next(key, value)) {
			String symbolValue = getSymbol(value.toString());
			if(!symbolValue.contains("0:"))
			{
				symbolValue = symbolList.get(Double.parseDouble(symbolValue));
				pw.println(symbolValue+ "," + key.toString());
			}
			
			
		}
		pw.close();
		reader.close();
	}
	
	public static String getSymbol(String input){
		
		String updatedValue = input.replaceAll("1.0:", " ");
		String midValue[] = updatedValue.split(",", 2);
		String midValue0 = midValue[0].replace('[', ' ').trim();
		return midValue0;
	}

}


