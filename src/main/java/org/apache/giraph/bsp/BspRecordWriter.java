package org.apache.giraph.bsp;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Used by {@link BspOutputFormat} since some versions of Hadoop
 * require that a RecordWriter is returned from getRecordWriter.
 * Does nothing, except insures that write is never called.
 */
public class BspRecordWriter extends RecordWriter<Text, Text> {

    @Override
    public void close(TaskAttemptContext context)
            throws IOException, InterruptedException {
        // Do nothing
    }

    @Override
    public void write(Text key, Text value)
            throws IOException, InterruptedException {
        throw new IOException("write: Cannot write with " +
                              getClass().getName() +
                              ".  Should never be called");
    }
}
