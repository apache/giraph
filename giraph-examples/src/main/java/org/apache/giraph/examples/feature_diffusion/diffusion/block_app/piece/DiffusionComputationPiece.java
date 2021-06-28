package org.apache.giraph.examples.feature_diffusion.diffusion.block_app.piece;

import java.util.Iterator;

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerHandle;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.combiner.SumMessageCombiner;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.reducers.impl.MaxReduce;
import org.apache.giraph.reducers.impl.OrReduce;
import org.apache.giraph.reducers.impl.SumReduce;
import org.apache.giraph.types.ops.IntTypeOps;
import org.apache.giraph.types.ops.LongTypeOps;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.giraph.examples.feature_diffusion.datastructures.DiffusionVertexValue;
import org.apache.giraph.examples.feature_diffusion.diffusion.DiffusionConstants;

public class DiffusionComputationPiece
    extends Piece<LongWritable, DiffusionVertexValue, NullWritable, IntWritable, Object> {

  // ADDED CONSTANTS
  ReducerHandle<LongWritable, LongWritable> superstepReducerHandle;

  // EXISTING CONSTANTS

  ReducerHandle<LongWritable, LongWritable> convincedVerticesHandle;
  ReducerHandle<LongWritable, LongWritable> usingVerticesHandle;
  ReducerHandle<LongWritable, LongWritable> deadVerticesHandle;
  ReducerHandle<LongWritable, LongWritable> latestActivationsHandle;
  ReducerHandle<LongWritable, LongWritable> hesitantVerticesHandle;

  // for KCORE (or general label) based algorithm
  ReducerHandle<LongWritable, LongWritable> invitedVerticesHandle;
  ReducerHandle<LongWritable, LongWritable> almostConvincedVerticesHandle;
  ReducerHandle<LongWritable, LongWritable>
      currentLabelHandle; // label da analizzare nello specifico superstep
  ReducerHandle<LongWritable, LongWritable>
      nextLabelHandle; // ogni volta riceve tutte le label ancora da eseguire
  ReducerHandle<BooleanWritable, BooleanWritable> timeToSwitchHandle;

  // for MIN_NUMBER based algorithm
  ReducerHandle<LongWritable, LongWritable> potentialVerticesHandle;
  ReducerHandle<LongWritable, LongWritable> oldInvitedVerticesHandle;
  ReducerHandle<LongWritable, LongWritable> oldConvincedVerticesHandle;
  ReducerHandle<LongWritable, LongWritable> oldDeadVerticesHandle;
  ReducerHandle<LongWritable, LongWritable> oldUsingVerticesHandle;
  ReducerHandle<LongWritable, LongWritable> oldHesitantVerticesHandle;
  ReducerHandle<LongWritable, LongWritable> oldAlmostConvincedVerticesHandle;
  ReducerHandle<BooleanWritable, BooleanWritable> justChangedTimeToSwitchHandle;

  // GLOBAL VARIABLES
  long superstep = 0;

  long currentLabel;
  boolean byLabel;
  boolean timeToSwitch;
  long nextLabel;

  int minNumber;
  double kSwitchThreshold;

  long convincedVertices;
  long usingVertices;
  long deadVertices;
  long invitedVertices;
  long almostConvincedVertices;
  long hesitantVertices;
  long oldInvitedVertices;
  private long oldConvincedVertices;
  private long oldDeadVertices;
  private long oldHesitantVertices;
  private long oldUsingVertices;
  private long oldAlmostConvincedVertices;

  ObjectTransfer<LongWritable> setupMaxLabel;
  ObjectTransfer<Boolean> stoppingCondition;

  boolean oldMaxLabelRetrieved = false;

  private boolean justChangedTimeToSwitch;

  public DiffusionComputationPiece(
      ObjectTransfer<LongWritable> oldMaxLabel, ObjectTransfer<Boolean> stoppingCondition) {
    this.setupMaxLabel = oldMaxLabel;
    this.stoppingCondition = stoppingCondition;
  }

  @Override
  public void registerReducers(CreateReducersApi reduceApi, Object executionStage) {
    super.registerReducers(reduceApi, executionStage);

    byLabel = reduceApi.getConf().getBoolean("ByLabel", true);
    kSwitchThreshold = (double) reduceApi.getConf().getFloat("KSwitchThreshold", (float) 0.7);

    convincedVerticesHandle =
        reduceApi.createLocalReducer(new SumReduce<LongWritable>(LongTypeOps.INSTANCE));
    usingVerticesHandle =
        reduceApi.createLocalReducer(new SumReduce<LongWritable>(LongTypeOps.INSTANCE));
    deadVerticesHandle =
        reduceApi.createLocalReducer(new SumReduce<LongWritable>(LongTypeOps.INSTANCE));
    invitedVerticesHandle =
        reduceApi.createLocalReducer(new SumReduce<LongWritable>(LongTypeOps.INSTANCE));
    almostConvincedVerticesHandle =
        reduceApi.createLocalReducer(new SumReduce<LongWritable>(LongTypeOps.INSTANCE));
    nextLabelHandle =
        reduceApi.createLocalReducer(new MaxReduce<LongWritable>(LongTypeOps.INSTANCE));
    currentLabelHandle =
        reduceApi.createLocalReducer(new MaxReduce<LongWritable>(LongTypeOps.INSTANCE));
    timeToSwitchHandle = reduceApi.createLocalReducer(new OrReduce());
    hesitantVerticesHandle =
        reduceApi.createLocalReducer(new SumReduce<LongWritable>(LongTypeOps.INSTANCE));

    if (!byLabel) {
      minNumber = reduceApi.getConf().getInt("minNumber", 200);
      potentialVerticesHandle =
          reduceApi.createLocalReducer(new SumReduce<LongWritable>(LongTypeOps.INSTANCE));
      oldInvitedVerticesHandle =
          reduceApi.createLocalReducer(new SumReduce<LongWritable>(LongTypeOps.INSTANCE));
      oldConvincedVerticesHandle =
          reduceApi.createLocalReducer(new SumReduce<LongWritable>(LongTypeOps.INSTANCE));
      oldDeadVerticesHandle =
          reduceApi.createLocalReducer(new SumReduce<LongWritable>(LongTypeOps.INSTANCE));
      oldUsingVerticesHandle =
          reduceApi.createLocalReducer(new SumReduce<LongWritable>(LongTypeOps.INSTANCE));
      oldHesitantVerticesHandle =
          reduceApi.createLocalReducer(new SumReduce<LongWritable>(LongTypeOps.INSTANCE));
      oldAlmostConvincedVerticesHandle =
          reduceApi.createLocalReducer(new SumReduce<LongWritable>(LongTypeOps.INSTANCE));
      justChangedTimeToSwitchHandle = reduceApi.createLocalReducer(new OrReduce());
    }
  }

  @Override
  public void masterCompute(BlockMasterApi master, Object executionStage) {
    super.masterCompute(master, executionStage);

    convincedVertices = convincedVerticesHandle.getReducedValue(master).get();
    usingVertices = usingVerticesHandle.getReducedValue(master).get();
    deadVertices = deadVerticesHandle.getReducedValue(master).get();
    invitedVertices = invitedVerticesHandle.getReducedValue(master).get();
    almostConvincedVertices = almostConvincedVerticesHandle.getReducedValue(master).get();
    hesitantVertices = hesitantVerticesHandle.getReducedValue(master).get();

    if (oldMaxLabelRetrieved) { // First time the new label is transferred from another piece
      if (timeToSwitch) nextLabel = nextLabelHandle.getReducedValue(master).get();
    } else {
      nextLabel = setupMaxLabel.get().get();
      oldMaxLabelRetrieved = true;
    }

    // This avoid having counters' value "0" when it's timeToSwitch and so the computation based on
    // MIN_NUMBER is "paused"
    if (!byLabel && timeToSwitch) {
      almostConvincedVertices = oldAlmostConvincedVerticesHandle.getReducedValue(master).get();
      invitedVertices = oldInvitedVerticesHandle.getReducedValue(master).get();
      usingVertices = oldUsingVerticesHandle.getReducedValue(master).get();
      deadVertices = oldDeadVerticesHandle.getReducedValue(master).get();
      convincedVertices = oldConvincedVerticesHandle.getReducedValue(master).get();
      hesitantVertices = oldHesitantVerticesHandle.getReducedValue(master).get();
    }

    master
        .getCounter(
            DiffusionConstants.activatedVerticesCounterGroup,
            DiffusionConstants.hesitantVerticesAggregator + superstep)
        .setValue(hesitantVertices);
    master
        .getCounter(
            DiffusionConstants.activatedVerticesCounterGroup,
            DiffusionConstants.usingVerticesCounter + superstep)
        .setValue(usingVertices);
    master
        .getCounter(
            DiffusionConstants.activatedVerticesCounterGroup,
            DiffusionConstants.convincedVerticesCounter + superstep)
        .setValue(convincedVertices);
    master
        .getCounter(
            DiffusionConstants.activatedVerticesCounterGroup,
            DiffusionConstants.deadVerticesCounter + superstep)
        .setValue(deadVertices);
    master
        .getCounter(
            DiffusionConstants.activatedVerticesCounterGroup,
            DiffusionConstants.invitedVerticesAggregator + superstep)
        .setValue(invitedVertices);
    master
        .getCounter(
            DiffusionConstants.activatedVerticesCounterGroup,
            DiffusionConstants.almostConvincedVerticesAggregator + superstep)
        .setValue(almostConvincedVertices);
    master
        .getCounter(
            DiffusionConstants.activatedVerticesCounterGroup,
            DiffusionConstants.currentLabel + superstep)
        .setValue(currentLabel);

    if (byLabel) { // Kcore or similar
      if (superstep >= 0) {
        if (timeToSwitch) timeToSwitch = false;
        if (superstep == 0) {
          currentLabel = nextLabel;
          nextLabel = -1;
          timeToSwitch = true;
        } else if (currentLabel != 1) { // if we haven't reached the lowest coreness
          long almostConvicedVal = almostConvincedVerticesHandle.getReducedValue(master).get();
          long invitedVal = invitedVerticesHandle.getReducedValue(master).get();
          // if the threshold is reached or all the invited vertices are dead, convinced or hesitant
          if (((double) almostConvicedVal) / invitedVal > kSwitchThreshold
              || invitedVertices == (deadVertices + convincedVertices + hesitantVertices)) {
            timeToSwitch = true;
            currentLabel = nextLabel;
            nextLabel = -1;
          }
        }
      }
    } else { // degree, pagerank or other similar where the label does not represent a group of
             // vertices
      justChangedTimeToSwitch = false;
      if (superstep == 0) {
        currentLabel = nextLabel;
        timeToSwitch = true;
        oldInvitedVertices = oldInvitedVerticesHandle.getReducedValue(master).get();
      }
      if (superstep > 0) {

        if (!timeToSwitch) {
          if (currentLabel > 0) {
            // if the threshold is reached or all the invited vertices are dead, convinced or
            // hesitant
            if (((double) convincedVertices) / invitedVertices > kSwitchThreshold
                || invitedVertices == (deadVertices + convincedVertices + hesitantVertices)) {
              timeToSwitch = true;
              oldInvitedVertices = invitedVertices;
              oldConvincedVertices = convincedVertices;
              oldAlmostConvincedVertices = almostConvincedVertices;
              oldDeadVertices = deadVertices;
              oldHesitantVertices = hesitantVertices;
              oldUsingVertices = usingVertices;
            }
          }
        } else { // it's time to switch: let's scan some label until we find at least <minNumber>
          // vertices more than now
          long old = oldInvitedVertices;
          long actual = potentialVerticesHandle.getReducedValue(master).get();
          if (actual - old > minNumber) {
            timeToSwitch = false;
            justChangedTimeToSwitch = true;
          } else if (nextLabelHandle.getReducedValue(master).get() < 0
              && superstep
                  > 10) { // reached the lowest label without finding at least MIN_NUMBER vertices
            timeToSwitch = false;
            justChangedTimeToSwitch = true;
            currentLabel = 0;
          } else { // continue to scan
            currentLabel = nextLabel;
            nextLabel = -1;
          }
        }
      }
    }

    superstep++;

    stoppingCondition.apply(
        superstep > 0
            && master.getTotalNumVertices()
                == (deadVertices + convincedVertices + hesitantVertices));
  }

  @Override
  public VertexSender<LongWritable, DiffusionVertexValue, NullWritable> getVertexSender(
      BlockWorkerSendApi<LongWritable, DiffusionVertexValue, NullWritable, IntWritable> workerApi,
      Object executionStage) {
    return (vertex) -> {
      DiffusionVertexValue value = vertex.getValue();
      if (timeToSwitch && superstep > 0)
        if (value.getLabel() < currentLabel)
          nextLabelHandle.reduce(new LongWritable(value.getLabel()));
      if (value.isVertexInvited(currentLabel) && superstep > 1) {
        int activeNeighbors = value.getActiveNeighbors();
        if (byLabel) {
          if (!value.isVertexDead()
              && superstep != 1
              && activeNeighbors == value.getVertexThreshold())
            hesitantVerticesHandle.reduce(new LongWritable(activeNeighbors));
          aggregateVerticesBasedOnProbability(value);
        } else {
          if (!timeToSwitch) {
            if (!value.isVertexDead()
                && !justChangedTimeToSwitch
                && activeNeighbors == value.getVertexThreshold())
              hesitantVerticesHandle.reduce(new LongWritable(1));
            aggregateVerticesBasedOnProbability(value);
          } else potentialVerticesHandle.reduce(new LongWritable(1));
        }
      }
      value.reset();

      if (value.isVertexInvited(currentLabel) && value.rollActivationDice() && superstep > 0)
        if (byLabel || (!byLabel && !timeToSwitch)) {
          usingVerticesHandle.reduce(new LongWritable(1));
          workerApi.sendMessageToAllEdges(vertex, new IntWritable(1));
        }
    };
  }

  @Override
  public VertexReceiver<LongWritable, DiffusionVertexValue, NullWritable, IntWritable>
      getVertexReceiver(BlockWorkerReceiveApi<LongWritable> workerApi, Object executionStage) {
    return (vertex, messages) -> {
      DiffusionVertexValue value = vertex.getValue();

      if (value.isVertexInvited(currentLabel)) {
        if (byLabel) {
          if (!value.isVertexDead()
              && superstep != 1) { // Update the using probability, if not dead
            value.setActiveNeighbors(checkMsgsAndUpdateProbability(messages, value));
          }
        } else {
          if (!timeToSwitch) {
            // Update the using probability if not dead and the computation has not just became
            // active
            // (because we don't have old messages sent so it would wrongly decrease the
            // probability)
            if (!value.isVertexDead() && !justChangedTimeToSwitch) {
              value.setActiveNeighbors(checkMsgsAndUpdateProbability(messages, value));
            }
          }
        }
      }
    };
  }

  /**
   * Check all the messages received by the vertex and update its probability with respect to the
   * its threshold
   *
   * @param msgs the list of messages received by the vertex
   * @param value the vertex value
   * @return the number of neighbors using the feature (invited and active)
   */
  private int checkMsgsAndUpdateProbability(
      Iterable<IntWritable> msgs, DiffusionVertexValue value) {
    Iterator<IntWritable> it = msgs.iterator();
    int activeNeighbors = 0;
    while (it.hasNext()) activeNeighbors += it.next().get();
    if (activeNeighbors > value.getVertexThreshold()) value.modifyCurrentActivationProbability(1);
    else if (activeNeighbors < value.getVertexThreshold())
      value.modifyCurrentActivationProbability(-1);
    return activeNeighbors;
  }

  /**
   * Basing on the vertex current activation probability, update the relative reducers
   *
   * @param value the vertex value, containing the current activation probability
   */
  private void aggregateVerticesBasedOnProbability(DiffusionVertexValue value) {
    if (value.isVertexConvinced()) convincedVerticesHandle.reduce(new LongWritable(1));
    if (value.isVertexDead()) { // Dead aggregator update
      deadVerticesHandle.reduce(new LongWritable(1));
    }
    if (value.isAlmostConvinced()) {
      almostConvincedVerticesHandle.reduce(new LongWritable(1));
    }
    invitedVerticesHandle.reduce(new LongWritable(1));
  }

  @Override
  public MessageCombiner<? super LongWritable, IntWritable> getMessageCombiner(
      ImmutableClassesGiraphConfiguration conf) {
    return new SumMessageCombiner(IntTypeOps.INSTANCE);
  }
}
