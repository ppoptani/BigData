package com.mapreduce;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class GetAllFileNames {

	public static void main(String args[])throws Exception {
		List<String> results = new ArrayList<String>();


		File[] files = new File("stocks").listFiles();
		//If this pathname does not denote a directory, then listFiles() returns null. 

		for (File file : files) {
		    if (file.isFile()) {
		        results.add(file.getName());
		        System.out.println(file.getName());
		    }
		}
	}
}
