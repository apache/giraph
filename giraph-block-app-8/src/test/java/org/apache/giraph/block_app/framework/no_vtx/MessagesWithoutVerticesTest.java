package org.apache.giraph.block_app.framework.no_vtx;

import java.util.Iterator;

import org.apache.giraph.block_app.framework.AbstractBlockFactory;
import org.apache.giraph.block_app.framework.BlockUtils;
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.block_app.test_setup.TestGraphUtils;
import org.apache.giraph.block_app.test_setup.graphs.EachVertexInit;
import org.apache.giraph.block_app.test_setup.graphs.Small1GraphInit;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.types.ops.collections.array.WIntArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterables;

public class MessagesWithoutVerticesTest {

  @Test
  public void testWithLocalBlockRunner() throws Exception {
    testSumOverSameGroup(3, false);
  }

  @Test
  public void testWithGiraphEnv() throws Exception {
    testSumOverSameGroup(3, true);
  }

  private void testSumOverSameGroup(int max, boolean fullGiraphEnv) throws Exception {
    TestGraphUtils.runTest(
        TestGraphUtils.chainModifiers(
            new Small1GraphInit<IntWritable, IntWritable, NullWritable>(),
            new EachVertexInit<>((vertex) -> vertex.getValue().set(- vertex.getId().get() % max))),
        (graph) -> {
          int[] sums = new int[max];
          for (int i = 0; i < max; i ++) {
            sums[i] -= i;
          }

          for (Vertex<IntWritable, IntWritable, NullWritable> vtx : graph.getTestGraph()) {
            sums[vtx.getId().get() % max] += vtx.getId().get();
          }

          for (Vertex<IntWritable, IntWritable, NullWritable> vtx : graph.getTestGraph()) {
            Assert.assertEquals(sums[vtx.getId().get() % max], vtx.getValue().get());
          }
        },
        (GiraphConfiguration conf) -> {
          BlockUtils.setBlockFactoryClass(conf, MessagesWithoutVerticesBlockFactory.class);
          TestGraphUtils.USE_FULL_GIRAPH_ENV_IN_TESTS.set(conf, fullGiraphEnv);
        });
  }

  public static class MessagesWithoutVerticesBlockFactory extends AbstractBlockFactory<Object> {
    @Override
    public Block createBlock(GiraphConfiguration conf) {
      ObjectTransfer<Iterable<IntWritable>> msgsTransfer = new ObjectTransfer<>();
      return new SequenceBlock(
        new Piece<IntWritable, IntWritable, NullWritable, IntWritable, Object>() {
          @Override
          public VertexSender<IntWritable, IntWritable, NullWritable> getVertexSender(
              BlockWorkerSendApi<IntWritable, IntWritable, NullWritable, IntWritable> workerApi,
              Object executionStage) {
            return (vtx) -> {
              System.out.println("send: " + vtx);
              System.out.flush();
              workerApi.sendMessage(vtx.getValue(), vtx.getId());
            };
          }

          @Override
          public VertexReceiver<IntWritable,IntWritable,NullWritable,IntWritable> getVertexReceiver(
              BlockWorkerReceiveApi<IntWritable> workerApi, Object executionStage) {
            return (vtx, msgs) -> {
              System.out.println("receive: " + vtx + " " + Iterables.toString(msgs));
              System.out.flush();
              Assert.assertFalse("" + vtx.getId(), Iterables.isEmpty(msgs));
              msgsTransfer.apply(msgs);
            };
          }

          @Override
          protected Class<IntWritable> getMessageClass() {
            return IntWritable.class;
          }

          @Override
          protected boolean receiveIgnoreExistingVertices() {
            return true;
          }
        },
        new Piece<IntWritable, IntWritable, NullWritable, IntWritable, Object>() {
          @Override
          public VertexSender<IntWritable, IntWritable, NullWritable> getVertexSender(
              BlockWorkerSendApi<IntWritable, IntWritable, NullWritable, IntWritable> workerApi,
              Object executionStage) {
            return (vtx) -> {
              WIntArrayList received = new WIntArrayList();
              int sum = vtx.getId().get();
              for (IntWritable msg : msgsTransfer.get()) {
                received.add(msg.get());
                sum += msg.get();
              }
              workerApi.sendMessageToMultipleEdges(received.fastIteratorW(), new IntWritable(sum));
            };
          }

          @Override
          public VertexReceiver<IntWritable,IntWritable,NullWritable,IntWritable> getVertexReceiver(
              BlockWorkerReceiveApi<IntWritable> workerApi, Object executionStage) {
            return (vtx, msgs) -> {
              Iterator<IntWritable> iter = msgs.iterator();
              vtx.getValue().set(iter.next().get());
              Assert.assertFalse(iter.hasNext());
            };
          }

          @Override
          protected Class<IntWritable> getMessageClass() {
            return IntWritable.class;
          }
        }
      );
    }

    @Override
    public Object createExecutionStage(GiraphConfiguration conf) {
      return new Object();
    }

    @Override
    protected Class<IntWritable> getVertexIDClass(GiraphConfiguration conf) {
      return IntWritable.class;
    }

    @Override
    protected Class<IntWritable> getVertexValueClass(GiraphConfiguration conf) {
      return IntWritable.class;
    }

    @Override
    protected Class<NullWritable> getEdgeValueClass(GiraphConfiguration conf) {
      return NullWritable.class;
    }
  }
}
