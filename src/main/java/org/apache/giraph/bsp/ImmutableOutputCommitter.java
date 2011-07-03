package org.apache.giraph.bsp;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This output committer doesn't do anything, meant for the case
 * where output isn't desired, or as a base for not using
 * FileOutputCommitter.
 */
public class ImmutableOutputCommitter extends OutputCommitter {
    @Override
    public void abortTask(TaskAttemptContext context) throws IOException {
    }

    @Override
    public void commitTask(TaskAttemptContext context) throws IOException {
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext context)
            throws IOException {
        return false;
    }

    @Override
    public void setupJob(JobContext context) throws IOException {
    }

    @Override
    public void setupTask(TaskAttemptContext context) throws IOException {
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
    }
}
