package com.yahoo.hadoop_bsp;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Use this to load data for a BSP application.  Note that the InputSplit must
 * also implement Writable.  The InputSplits will determine the partitioning of
 * vertices across the mappers, so keep that in consideration when implementing
 * getSplits().
 * @author aching
 *
 * @param <I>
 * @param <V>
 * @param <E>
 */
public interface VertexInputFormat<I, V, E> {
	/** 
	 * Logically split the vertices for a BSP application.  
	 * 
	 * <p>Each {@link InputSplit} is then assigned to an individual 
	 * {@link Mapper} for processing.</p>
	 *
	 * <p><i>Note</i>: The split is a <i>logical</i> split of the inputs and the
	 * input files are not physically split into chunks. For e.g. a split could
	 * be <i>&lt;input-file-path, start, offset&gt;</i> tuple. The InputFormat
	 * also creates the {@link VertexReader} to read the {@link InputSplit}.
	 * 
	 * @param conf configuration
	 * @param numSplits number of splits for the input
	 * @return an array of {@link InputSplit}s for the job.
	 */	
	public List<InputSplit> getSplits(Configuration conf, int numSplits) 
		throws IOException, InterruptedException;
	
	/**
	 * Create a vertex reader for a given split. The framework will call
	 * {@link VertexReader#initialize(InputSplit, TaskAttemptContext)} before
	 * the split is used.
	 * @param split the split to be read
	 * @param context the information about the task
	 * @return a new record reader
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public VertexReader<I, V, E> createVertexReader(InputSplit split,
		TaskAttemptContext context) throws IOException; 
}
