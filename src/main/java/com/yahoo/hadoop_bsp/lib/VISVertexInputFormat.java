package com.yahoo.hadoop_bsp.lib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.yahoo.hadoop_bsp.BspInputSplit;
import com.yahoo.hadoop_bsp.VertexInputFormat;
import com.yahoo.hadoop_bsp.VertexReader;

/**
 * This VertexInputFormat is meant for reading a VIS graph,
 * which lists vertices with their edges on single lines.
 *
 */
public class VISVertexInputFormat extends TextInputFormat implements 
	VertexInputFormat<Text, DoubleWritable, Float> {

  protected long splitSize = 1;

  protected long getFormatMinSplitSize() {
      return splitSize;
  }

	public List<InputSplit> getSplits(Configuration conf, int numSplits)
		throws IOException, InterruptedException {

      if (numSplits == 0) {
          numSplits = 1;
      }
      JobContext job = new JobContext(conf, new JobID());
      long totalLen = 0;
      int numFiles = 0;
      for (FileStatus file: listStatus(job)) {
          Path path = file.getPath();
          FileSystem fs = path.getFileSystem(job.getConfiguration());
          totalLen += file.getLen();
          numFiles++;
      }
      if (numFiles == 0) {
          throw new RuntimeException("No files in input directory.");
      }
      if (numSplits < numFiles) {
          throw new RuntimeException("Number of splits=" + numSplits +
                    " smaller than number of input files (" + numFiles +
                    ") not supported.");
      }
      // setting both minimum and maximum split size to same value
      splitSize = totalLen / (numSplits - numFiles + 1);
      job.getConfiguration().setLong("mapred.max.split.size", splitSize);

      List<InputSplit> inputSplitList = getSplits(job);
      if (inputSplitList.size() > numSplits) {
          throw new RuntimeException("VISVertexInputFormat returns #splits="
                    + inputSplitList.size() + " > " + numSplits);
      }
      return inputSplitList;
	}
	
	public VertexReader<Text, DoubleWritable, Float> createVertexReader(
		    InputSplit split, TaskAttemptContext context) 
		    throws IOException {
		  return new VISVertexReader();
	}

}
