package org.apache.giraph.bsp;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.Text;

/**
 * Only returns a single key-value pair so that the map() can run.
 */
class BspRecordReader extends RecordReader<Text, Text> {
    /** Has the one record been seen? */
    private boolean seenRecord = false;

    @Override
    public void close() throws IOException {
        return;
    }

    @Override
    public float getProgress() throws IOException {
        if (seenRecord == true) {
            return 1f;
        }
        else {
            return 0f;
        }
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return new Text("only key");
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return new Text("only value");
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (seenRecord == false) {
            seenRecord = true;
            return true;
        }
        else {
            return false;
        }
    }
}
