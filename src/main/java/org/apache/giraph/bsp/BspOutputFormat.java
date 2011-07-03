package org.apache.giraph.bsp;

import java.io.IOException;

import org.apache.giraph.graph.BspUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

/**
 * This is for internal use only.  Allows the vertex output format routines
 * to be called as if a normal Hadoop job.
 */
public class BspOutputFormat extends OutputFormat<Text, Text> {
    /** Class logger */
    private static Logger LOG = Logger.getLogger(BspOutputFormat.class);

    @Override
    public void checkOutputSpecs(JobContext context)
            throws IOException, InterruptedException {
        if (BspUtils.getVertexOutputFormatClass(context.getConfiguration())
                == null) {
            LOG.warn("checkOutputSpecs: ImmutableOutputCommiter " +
                     " will not check anything");
            return;
        }
        BspUtils.createVertexOutputFormat(context.getConfiguration()).
            checkOutputSpecs(context);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        if (BspUtils.getVertexOutputFormatClass(context.getConfiguration())
                == null) {
            LOG.warn("getOutputCommitter: Returning " +
                     "ImmutableOutputCommiter (does nothing).");
            return new ImmutableOutputCommitter();
        }
        return BspUtils.createVertexOutputFormat(context.getConfiguration()).
            getOutputCommitter(context);
    }

    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return null;
    }
}
