package org.apache.giraph.examples.feature_diffusion.labeling.factory;

import org.apache.giraph.block_app.framework.AbstractBlockFactory;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.RepeatBlock;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.TypesHolder;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.giraph.examples.feature_diffusion.datastructures.LabelingVertexValue;

public class PageRankLabelingBlockFactory extends AbstractBlockFactory<Object>
    implements TypesHolder<
        LongWritable, LabelingVertexValue, NullWritable, DoubleWritable, DoubleWritable> {

  long numVerticesTotal;

  private ConsumerWithVertex<
          LongWritable, LabelingVertexValue, NullWritable, Iterable<DoubleWritable>>
      pageRankConsumer =
          (vertex, messages) -> {
            LabelingVertexValue value = vertex.getValue();
            double sum = 0;
            for (DoubleWritable message : messages) {
              if (message.get() >= 0) sum += message.get();
            }
            double pr = ((0.15f / numVerticesTotal) + 0.85f * sum);
            value.setTemp(pr); // to change, removing
          };

  @Override
  public Block createBlock(GiraphConfiguration conf) {
    return new SequenceBlock(
        Pieces.masterCompute(
            "Master preprocess",
            (master) -> {
              numVerticesTotal = master.getTotalNumVertices();
            }),
        new RepeatBlock(
            conf.getInt("labeling.pagerank.iterations", 49),
            Pieces
                .<LongWritable, LabelingVertexValue, NullWritable, DoubleWritable>
                    sendMessageToNeighbors(
                        "PageRank labeling",
                        DoubleWritable.class,
                        (vertex) -> {
                          LabelingVertexValue value = vertex.getValue();
                          return new DoubleWritable(value.getTemp() / vertex.getNumEdges());
                        },
                        pageRankConsumer)),
        Pieces.<LongWritable, LabelingVertexValue, NullWritable>forAllVertices(
            "Closing PageRank",
            (vertex) -> {
              LabelingVertexValue value = vertex.getValue();
              int cif = (int) (Math.log10(numVerticesTotal) + 2);
              value.setLabel((long) (value.getTemp() * Math.pow(10, cif)));
            }));
  }

  @Override
  protected Class<NullWritable> getEdgeValueClass(GiraphConfiguration arg0) {
    return NullWritable.class;
  }

  @Override
  protected Class<LongWritable> getVertexIDClass(GiraphConfiguration arg0) {
    return LongWritable.class;
  }

  @Override
  protected Class<LabelingVertexValue> getVertexValueClass(GiraphConfiguration arg0) {
    return LabelingVertexValue.class;
  }

  @Override
  public Object createExecutionStage(GiraphConfiguration arg0) {
    return new Object();
  }
}
