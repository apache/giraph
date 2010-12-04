package com.yahoo.hadoop_bsp.examples;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.yahoo.hadoop_bsp.BspJob;
import com.yahoo.hadoop_bsp.HadoopVertex;
import com.yahoo.hadoop_bsp.VertexInputFormat;
import com.yahoo.hadoop_bsp.VertexWriter;

/**
 * test for simple App.
 */
public class VISBspMainTest {
	
	/**
	 * Sample BSP application.
	 * 
	 * @author aching
	 *
	 * @param <V>
	 * @param <E>
	 * @param <M>
	 */
	public static final class TestBsp extends 
            HadoopVertex<Text, DoubleWritable, Float, DoubleWritable> {
      public void compute(Iterator<DoubleWritable> msgIterator) {
          if (getSuperstep() >= 1) {
              double sum = 0;
              while (msgIterator.hasNext()) {
                  sum += msgIterator.next().get();
              }
              double vertexValue = (0.1f / getNumVertices()) + 0.9 * sum;
              if (getSuperstep() < 50) {
                  long edges = getNumEdges();
                  sentMsgToAllEdges(new DoubleWritable(vertexValue/edges));
              } else {
                  setVertexValue(new DoubleWritable(vertexValue));
                  voteToHalt();
              }
          }
      }
  }
	
  /**
   * Run a sample BSP job.
   * @throws IOException
   * @throws ClassNotFoundException 
   * @throws InterruptedException 
   */

  public static void main(String[] args)
        throws IOException, InterruptedException, ClassNotFoundException {
      Configuration conf = new Configuration();

      conf.set(BspJob.BSP_ZOOKEEPER_LIST, "localhost:2181");
      conf.setFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 100.0f);
      conf.setInt(BspJob.BSP_POLL_ATTEMPTS, 5);
      conf.setInt(BspJob.BSP_POLL_MSECS, 5*1000);
      conf.setInt(BspJob.BSP_RPC_INITIAL_PORT, BspJob.BSP_RPC_DEFAULT_PORT);
      FileSystem hdfs = FileSystem.get(conf);
    	conf.setClass("bsp.vertexClass", TestBsp.class, HadoopVertex.class);
    	conf.setClass("bsp.msgValueClass", DoubleWritable.class, Writable.class);
    	conf.setClass("bsp.inputSplitClass", 
    				  FileSplit.class, 
    				  InputSplit.class);
    	conf.setClass("bsp.vertexInputFormatClass", 
    				  VISVertexInputFormat.class,
    				  VertexInputFormat.class);
    	conf.setClass("bsp.vertexWriterClass", 
    				  VISVertexWriter.class,
    				  VertexWriter.class);
      conf.setClass("mapreduce.outputformat.class",
              TextOutputFormat.class,
              OutputFormat.class);
      conf.setClass("bsp.indexClass",
              Text.class,
              WritableComparable.class);
      conf.setClass("mapred.output.key.class",
              Text.class,
              Object.class);
      conf.setClass("mapred.output.value.class",
              NullWritable.class,
              Object.class);

      args = new GenericOptionsParser(conf, args).getRemainingArgs();
      Path outputPath = null;
      Path inputPath = null;

      try {
          for(int i=0; i < args.length; ++i) {
            if ("-outputDir".equals(args[i])) {
       	      outputPath = new Path((String)args[++i]);    	
    	      hdfs.delete(outputPath, true);
              System.out.println("-outputDir " + args[i]);
            } else
            if ("-inputDir".equals(args[i])) {
              inputPath = new Path((String)args[++i]);
              System.out.println("-inputDir " + args[i]);
            } else
            if ("-map".equals(args[i])) {
              int numMapTasks = Integer.parseInt(args[++i]);
              conf.setInt("mapred.map.tasks", numMapTasks);
              conf.setInt(BspJob.BSP_INITIAL_PROCESSES, numMapTasks);
              conf.setInt(BspJob.BSP_MIN_PROCESSES, numMapTasks);
              System.out.println("-map " + args[i]);
            } else
              System.out.println("unknwon option " + args[i]);
          }
      } catch (ArrayIndexOutOfBoundsException except) {
      }

      conf.set("keep.failed.task.files", "true");

    	BspJob<Integer, String, String> bspJob = 
    		new BspJob<Integer, String, String>(conf, "testBspJob");
    	FileOutputFormat.setOutputPath(bspJob, outputPath);
        FileInputFormat.setInputPaths(bspJob, inputPath);
    	bspJob.run();
  }
    
}
