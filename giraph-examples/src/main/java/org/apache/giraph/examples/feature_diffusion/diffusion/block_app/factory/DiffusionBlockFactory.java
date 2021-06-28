package org.apache.giraph.examples.feature_diffusion.diffusion.block_app.factory;

import org.apache.giraph.block_app.framework.AbstractBlockFactory;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.RepeatUntilBlock;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.TypesHolder;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.reducers.impl.MaxReduce;
import org.apache.giraph.types.ops.LongTypeOps;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import org.apache.giraph.examples.feature_diffusion.datastructures.DiffusionVertexValue;
import org.apache.giraph.examples.feature_diffusion.diffusion.block_app.piece.DiffusionComputationPiece;

public class DiffusionBlockFactory extends AbstractBlockFactory<Object>
    implements TypesHolder<LongWritable, DiffusionVertexValue, NullWritable, Writable, Writable> {

  ObjectTransfer<LongWritable> firstMaxLabel;
  ObjectTransfer<Boolean> stoppingCondition;

  double delta;
  String thresholdType;
  double initialActivationProbability;
  double almostConvincedThreshold;

  public DiffusionBlockFactory() {
    firstMaxLabel = new ObjectTransfer<LongWritable>();
    stoppingCondition = new ObjectTransfer<Boolean>();
  }

  @Override
  public Block createBlock(GiraphConfiguration conf) {
    delta = (double) conf.getFloat("Delta", (float) 0.005);
    thresholdType = conf.getStrings("ThresholdType", "")[0];

    initialActivationProbability = (double) conf.getFloat("InitialProbability", (float) 0.02);
    almostConvincedThreshold = (double) conf.getFloat("AlmostConvincedTreshold", (float) 0.7);

    DiffusionComputationPiece dcp = new DiffusionComputationPiece(firstMaxLabel, stoppingCondition);

    return new SequenceBlock(
        Pieces.<LongWritable, LongWritable, LongWritable, DiffusionVertexValue, NullWritable>reduce(
            "Setup block",
            new MaxReduce<LongWritable>(LongTypeOps.INSTANCE),
            (vertex) -> {
              return new LongWritable(setup(vertex.getValue()));
            },
            (value) -> {
              firstMaxLabel.apply(value);
            }),
        new RepeatUntilBlock(
            GiraphConstants.MAX_NUMBER_OF_SUPERSTEPS.get(conf), dcp, stoppingCondition));
  }

  @Override
  public Object createExecutionStage(GiraphConfiguration arg0) {
    return new Object();
  }

  @Override
  protected Class<? extends Writable> getEdgeValueClass(GiraphConfiguration arg0) {
    return NullWritable.class;
  }

  @Override
  protected Class<? extends WritableComparable> getVertexIDClass(GiraphConfiguration arg0) {
    return LongWritable.class;
  }

  @Override
  protected Class<? extends Writable> getVertexValueClass(GiraphConfiguration arg0) {
    return DiffusionVertexValue.class;
  }

  /**
   * Set the initial values for some vertex parameters
   *
   * @param value the vertex value on which operate
   * @return the vertex label
   */
  private long setup(DiffusionVertexValue value) {
    value.setDelta(delta);
    value.setInitialActivationProbability(initialActivationProbability);
    value.setAlmostConvincedTreshold(almostConvincedThreshold);
    if (thresholdType.compareTo("1") == 0) value.setVertexThreshold(1);
    else if (thresholdType.compareTo("Prop") == 0) {
      value.setVertexThreshold((int) value.getLabel() / 20);
    }
    return value.getLabel();
  }
}
