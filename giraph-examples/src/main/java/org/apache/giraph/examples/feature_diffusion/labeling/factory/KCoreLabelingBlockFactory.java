package org.apache.giraph.examples.feature_diffusion.labeling.factory;

import org.apache.giraph.block_app.framework.AbstractBlockFactory;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.RepeatUntilBlock;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.TypesHolder;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.giraph.reducers.impl.AndReduce;
import org.apache.giraph.writable.tuple.LongLongWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.giraph.examples.feature_diffusion.datastructures.LabelingVertexValue;

@SuppressWarnings("unused")
public class KCoreLabelingBlockFactory extends AbstractBlockFactory<Object>
    implements TypesHolder<
        LongWritable, LabelingVertexValue, NullWritable, LongLongWritable, LongLongWritable> {

  ObjectTransfer<Boolean> stoppingCondition;

  public static ConsumerWithVertex<
          LongWritable, LabelingVertexValue, NullWritable, Iterable<LongLongWritable>>
      kCoreConsumer =
          (vertex, messages) -> {
            LabelingVertexValue value = vertex.getValue();
            for (LongLongWritable msg : messages) {
              long id = msg.getLeft().get();
              int coreness = (int) msg.getRight().get();
              value.updateNeighboorLabel(id, coreness);
            }
          };

  @Override
  public Block createBlock(GiraphConfiguration conf) {
    stoppingCondition = new ObjectTransfer<Boolean>(false);
    return new SequenceBlock(
        Pieces
            .<LongWritable, LabelingVertexValue, NullWritable, LongLongWritable>
                sendMessageToNeighbors(
                    "KCore Labeling First run",
                    LongLongWritable.class,
                    (vertex) -> {
                      LabelingVertexValue value = vertex.getValue();
                      value.setLabel(Integer.max(vertex.getNumEdges(), 1));
                      value.setChanged(false);
                      return new LongLongWritable(vertex.getId().get(), value.getLabel());
                    },
                    kCoreConsumer),
        new RepeatUntilBlock(
            GiraphConstants.MAX_NUMBER_OF_SUPERSTEPS.get(conf),
            new SequenceBlock(
                Pieces
                    .<LongWritable, LabelingVertexValue, NullWritable, LongLongWritable>
                        sendMessageToNeighbors(
                            "KCore Labeling",
                            LongLongWritable.class,
                            (vertex) -> {
                              LabelingVertexValue value = vertex.getValue();
                              int tempLabel =
                                  computeIndex(value.getNeighborsLabel(), value.getLabel());
                              if (tempLabel < value.getLabel()) value.setLabel(tempLabel);
                              if (value.isChanged()) {
                                //														value.setChanged(false);
                                return new LongLongWritable(vertex.getId().get(), value.getLabel());
                              }
                              return null;
                            },
                            kCoreConsumer),
                Pieces
                    .<BooleanWritable, BooleanWritable, LongWritable, LabelingVertexValue,
                        NullWritable>
                        reduce(
                            "Reducing stopping Condition",
                            AndReduce.INSTANCE,
                            (vertex) -> {
                              boolean isLabelChanged = vertex.getValue().isChanged();
                              vertex.getValue().setChanged(false);
                              return new BooleanWritable(!isLabelChanged);
                            },
                            (value) -> {
                              stoppingCondition.apply(value.get());
                            })),
            stoppingCondition));
  }

  private int computeIndex(HashMap<Long, Long> neighborsLabel, long coreness) {
    int[] corenessCount = new int[(int) coreness];
    for (int i = 0; i < coreness; i++) corenessCount[i] = 0;
    for (Entry<Long, Long> pair : neighborsLabel.entrySet()) {
      long corenessCandidate = Long.min(pair.getValue(), coreness);
      corenessCount[(int) corenessCandidate - 1]++;
    }
    for (int i = (int) (coreness - 1); i > 0; i--) corenessCount[i - 1] += corenessCount[i];
    int i = (int) coreness;
    while (i > 1 && corenessCount[i - 1] < i) {
      i--;
    }
    return i;
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
