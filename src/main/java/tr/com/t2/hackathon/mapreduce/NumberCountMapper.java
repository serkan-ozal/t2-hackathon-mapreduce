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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 * @author Serkan OZAL
 * 
 * Map implementation of Map/Reduce job.
 * Reads input and generates partial intermediate results.
 */
public class NumberCountMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	private static final Logger logger = Logger.getLogger(NumberCountMapper.class); 

	private final static IntWritable ONE = new IntWritable(1);
	
    @Override
    public void run(Context context) throws IOException, InterruptedException {
    	init(context);
    	super.run(context);
    }

    protected void init(Context context) {
    	try {
    		logger.info("Mapper has been initialized ...");
    	}
    	catch (Throwable t) {
    		logger.error("Error occured while initializing Mapper", t);
    	}
    }

	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
        	String line = value.toString();
        	// Cast it to integer typed number. Because every line in input file consist of only one number.
    		int number = Integer.parseInt(line);
    		// Write context that this number exist 1 time for this line
    		context.write(new IntWritable(number), ONE);
        }
        catch (Throwable t) {
        	logger.error("Error occured while executing map function of Mapper", t);
        }
    }

}
