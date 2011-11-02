package org.apache.giraph.lib;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.VertexInputFormat;
import org.apache.giraph.graph.VertexReader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import java.io.IOException;
import java.util.List;

public class SequenceFileVertexInputFormat<I extends WritableComparable<I>,
                                           V extends Writable,
                                           E extends Writable,
                                           M extends Writable,
                                           X extends BasicVertex<I, V, E, M>>
    extends VertexInputFormat<I, V, E, M> {
  protected SequenceFileInputFormat<I, X> sequenceFileInputFormat
      = new SequenceFileInputFormat<I, X>();

  @Override public List<InputSplit> getSplits(JobContext context, int numWorkers)
      throws IOException, InterruptedException {
    return sequenceFileInputFormat.getSplits(context);
  }

  @Override
  public VertexReader<I, V, E, M> createVertexReader(InputSplit split,
      TaskAttemptContext context)
      throws IOException {
    return new SequenceFileVertexReader<I, V, E, M, X>(
        sequenceFileInputFormat.createRecordReader(split, context));
  }

  public static class SequenceFileVertexReader<I extends WritableComparable<I>,
      V extends Writable, E extends Writable, M extends Writable,
      X extends BasicVertex<I, V, E, M>>
      implements VertexReader<I, V, E, M> {
    private final RecordReader<I, X> recordReader;

    public SequenceFileVertexReader(RecordReader<I, X> recordReader) {
      this.recordReader = recordReader;
    }

    @Override public void initialize(InputSplit inputSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
      recordReader.initialize(inputSplit, context);
    }

    @Override public boolean nextVertex() throws IOException, InterruptedException {
      return recordReader.nextKeyValue();
    }

    @Override public BasicVertex<I, V, E, M> getCurrentVertex()
        throws IOException, InterruptedException {
      return recordReader.getCurrentValue();
    }


    @Override public void close() throws IOException {
      recordReader.close();
    }

    @Override public float getProgress() throws IOException, InterruptedException {
      return recordReader.getProgress();
    }
  }
}
