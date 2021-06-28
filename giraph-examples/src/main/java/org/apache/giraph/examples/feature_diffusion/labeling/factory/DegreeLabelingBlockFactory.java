package org.apache.giraph.examples.feature_diffusion.labeling.factory;

import org.apache.giraph.block_app.framework.AbstractBlockFactory;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.TypesHolder;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.giraph.examples.feature_diffusion.datastructures.LabelingVertexValue;

public class DegreeLabelingBlockFactory extends AbstractBlockFactory<Object>
    implements TypesHolder<
        LongWritable, LabelingVertexValue, NullWritable, NullWritable, NullWritable> {

  long superstep = 0;

  @Override
  public Block createBlock(GiraphConfiguration arg0) {
    return Pieces.<LongWritable, LabelingVertexValue, NullWritable>forAllVertices(
        "Degree Labeling",
        (vertex) -> {
          LabelingVertexValue value = vertex.getValue();
          value.setLabel(vertex.getNumEdges());
        });
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
