package com.yahoo.hadoop_bsp.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.yahoo.hadoop_bsp.BspInputSplit;
import com.yahoo.hadoop_bsp.VertexInputFormat;
import com.yahoo.hadoop_bsp.VertexReader;

/**
 * This VertexInputFormat is meant for testing/debugging.  It simply generates
 * some vertex data that can be consumed by test applications.
 * @author aching
 *
 */
public class TestVertexInputFormat implements 
	VertexInputFormat<Long, Integer, Float> {
	
	public List<InputSplit> getSplits(int numSplits)
		throws IOException, InterruptedException {
		/*
		 * This is meaningless, the VertexReader will generate all the test
		 * data.
		 */
        List<InputSplit> inputSplitList = new ArrayList<InputSplit>();
        for (int i = 0; i < numSplits; ++i) {
        	inputSplitList.add(new BspInputSplit(i, numSplits));
        }
        return inputSplitList;
	}
	
	public VertexReader<Long, Integer, Float> createRecordReader(
		InputSplit split, TaskAttemptContext context) 
		throws IOException {
		return new TestVertexReader();
	}

}
