package com.yahoo.hadoop_bsp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * Limits the functions that can be called by the user.  Job is too flexible
 * for our needs.
 * 
 * @author aching
 */
public class BspJob extends Job {

	public BspJob(
			Configuration conf, String jobName) throws IOException {
		super(conf, jobName);
	}
}
