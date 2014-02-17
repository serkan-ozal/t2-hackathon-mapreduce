/*
 * Copyright (c) 2014, "SkyKeeper Team". All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY 
 * KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
 * PARTICULAR PURPOSE.
 */

package tr.com.t2.hackathon.mapreduce;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

/**
 * @author Serkan OZAL
 * 
 * Starts Map/Reduce job. Runs on Master node and submits sub-job to slave nodes (mapper and reducer nodes)
 */
public class MapReduceDriver {

	private static final Logger logger = Logger.getLogger(MapReduceDriver.class);
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		long start, finish;
		long executionTimeInSeconds, executionTimeInMinutes;
		
		JobConf conf = new JobConf();
		
		Job job = new Job(conf, "NumberCounter");
		
		// Use SPACE instead of TAB as separator between key and value at output
		conf.set("mapred.textoutputformat.separator", " "); // Prior to Hadoop 2 (YARN)
        conf.set("mapreduce.textoutputformat.separator", " ");  // Hadoop v2+ (YARN)
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        conf.set("mapreduce.output.key.field.separator", " ");
        conf.set("mapred.textoutputformat.separatorText", " "); 
		
		job.setJarByClass(MapReduceDriver.class);
		 
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		 
		job.setMapperClass(NumberCountMapper.class);
		job.setReducerClass(NumberCountReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
	
		////////////////////////////////////////////////////////////////////////////////////
		
		// If output directory already exists, first remove it
		FileSystem outputFS = outputPath.getFileSystem(conf);
		while (true) {
			if (outputFS.exists(outputPath)) {
				outputFS.delete(outputPath, true);
				Thread.sleep(1000); 
			}
			else {
				break;
			}
		}
		logger.info("Deleted existing output path: " + outputPath.toUri().toString());

		////////////////////////////////////////////////////////////////////////////////////
		
		logger.info("MapReduce job started ...");
		start = System.currentTimeMillis();
		
		// Start job and wait it for completion
		job.waitForCompletion(true);
		
		finish = System.currentTimeMillis();
		executionTimeInSeconds = (finish - start) / 1000;
		executionTimeInMinutes = executionTimeInSeconds / 60;
		logger.info("MapReduce job finished in " + 
						executionTimeInSeconds + " seconds " + 
						"(" + executionTimeInMinutes + " minutes" + ")");
		
		////////////////////////////////////////////////////////////////////////////////////
		
		
		Properties props = System.getProperties();
		String outputFileName = props.getProperty("outputFileName");
		if (StringUtils.isEmpty(outputFileName)) {
			outputFileName = "output.txt";
		}
		Path resultPath = new Path(args[1] + "/" + outputFileName);
		
		logger.info("MapReduce output merging started ...");
		start = System.currentTimeMillis();
		
		// Merge all partitioned output files to single result file such as "output.txt" in output directory.
		FileUtil.copyMerge(outputFS, outputPath, outputFS, resultPath, false, conf, null);

		finish = System.currentTimeMillis();
		executionTimeInSeconds = (finish - start) / 1000;
		executionTimeInMinutes = executionTimeInSeconds / 60;
		logger.info("MapReduce output merging finished in " + 
						executionTimeInSeconds + " seconds " + 
						"(" + executionTimeInMinutes + " minutes" + ")");
	}

}
