package com.yahoo.hadoop_bsp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * This InputFormat supports the BSP model by ensuring that the user specifies
 * how many splits (number of mappers) should be started simultaneously.
 * @author aching
 *
 */
public class BspInputFormat extends InputFormat<Text, Text> {
	public List<InputSplit> getSplits(JobContext context) 
    	throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int minMapTasks = conf.getInt("bsp.minTasks", 0);
		if (minMapTasks <= 0) {
			throw new InterruptedException("Set bsp.minTasks > 0");
		}
        List<InputSplit> inputSplitList = new ArrayList<InputSplit>();
        for (int i = 0; i < minMapTasks; ++i) {
        	inputSplitList.add(new BspInputSplit());
        }
        return inputSplitList;
	}
    
	public RecordReader<Text, Text> 
		createRecordReader(InputSplit split, TaskAttemptContext context) 
		throws IOException, InterruptedException {
		return new BspRecordReader();
	}
}
