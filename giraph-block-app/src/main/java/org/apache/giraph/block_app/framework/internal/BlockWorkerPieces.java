/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.block_app.framework.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

import org.apache.giraph.block_app.framework.api.BlockApiHandle;
import org.apache.giraph.comm.messages.MessageEncodeAndStoreType;
import org.apache.giraph.conf.DefaultMessageClasses;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.MessageClasses;
import org.apache.giraph.factories.DefaultMessageValueFactory;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.types.NoMessage;
import org.apache.giraph.utils.UnsafeReusableByteArrayInput;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.giraph.writable.kryo.KryoWritableWrapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * Pair of pieces to be executed on workers in a superstep
 *
 * @param <S> Execution stage type
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class BlockWorkerPieces<S> {
  private static final Logger LOG = Logger.getLogger(BlockWorkerPieces.class);

  /** Aggregator holding next worker computation */
  private static final
  String NEXT_WORKER_PIECES = "giraph.blocks.next_worker_pieces";

  private final PairedPieceAndStage<S> receiver;
  private final PairedPieceAndStage<S> sender;
  private final BlockApiHandle blockApiHandle;

  public BlockWorkerPieces(
      PairedPieceAndStage<S> receiver, PairedPieceAndStage<S> sender,
      BlockApiHandle blockApiHandle) {
    this.receiver = receiver;
    this.sender = sender;
    this.blockApiHandle = blockApiHandle;
  }

  public PairedPieceAndStage<S> getReceiver() {
    return receiver;
  }

  public PairedPieceAndStage<S> getSender() {
    return sender;
  }

  public BlockApiHandle getBlockApiHandle() {
    return blockApiHandle;
  }

  public MessageClasses getOutgoingMessageClasses(
      ImmutableClassesGiraphConfiguration conf) {
    MessageClasses messageClasses;
    if (sender == null || sender.getPiece() == null) {
      messageClasses = new DefaultMessageClasses(
          NoMessage.class,
          DefaultMessageValueFactory.class,
          null,
          MessageEncodeAndStoreType.BYTEARRAY_PER_PARTITION);
    } else {
      messageClasses = sender.getPiece().getMessageClasses(conf);
    }

    messageClasses.verifyConsistent(conf);
    return messageClasses;
  }

  @Override
  public String toString() {
    return "[receiver=" + receiver + ",sender=" + sender + "]";
  }

  public String toStringShort() {
    String receiverString =
        Objects.toString(receiver != null ? receiver.getPiece() : null);
    String senderString =
        Objects.toString(sender != null ? sender.getPiece() : null);
    if (receiverString.equals(senderString)) {
      return "[receiver&sender=" + receiverString + "]";
    } else {
      return "[receiver=" + receiverString + ",sender=" + senderString + "]";
    }
  }

  /**
   * Sets which WorkerComputation is going to be executed in the next superstep.
   */
  public static <S> void setNextWorkerPieces(
      MasterCompute master, BlockWorkerPieces<S> nextWorkerPieces) {
    Writable toBroadcast = new KryoWritableWrapper<>(nextWorkerPieces);
    byte[] data = WritableUtils.toByteArrayUnsafe(toBroadcast);

    // TODO: extract splitting logic into common utility
    int overhead = 4096;
    int singleSize = Math.max(
        overhead,
        GiraphConstants.MAX_MSG_REQUEST_SIZE.get(master.getConf()) - overhead);

    ArrayList<byte[]> splittedData = new ArrayList<>();
    if (data.length < singleSize) {
      splittedData.add(data);
    } else {
      for (int start = 0; start < data.length; start += singleSize) {
        splittedData.add(Arrays.copyOfRange(
            data, start, Math.min(data.length, start + singleSize)));
      }
    }

    LOG.info("Next worker piece - total serialized size: " + data.length +
        ", split into " + splittedData.size());
    master.getContext().getCounter(
        "PassedWorker Stats", "total serialized size")
        .increment(data.length);
    master.getContext().getCounter(
        "PassedWorker Stats", "split parts")
        .increment(splittedData.size());

    master.broadcast(NEXT_WORKER_PIECES, new IntWritable(splittedData.size()));

    for (int i = 0; i < splittedData.size(); i++) {
      master.broadcast(NEXT_WORKER_PIECES + "_part_" + i,
          KryoWritableWrapper.wrapIfNeeded(splittedData.get(i)));
    }

    master.setOutgoingMessageClasses(
        nextWorkerPieces.getOutgoingMessageClasses(master.getConf()));
  }

  public static <S> BlockWorkerPieces<S> getNextWorkerPieces(
      WorkerGlobalCommUsage worker) {
    int splits = worker.<IntWritable>getBroadcast(NEXT_WORKER_PIECES).get();

    int totalLength = 0;
    ArrayList<byte[]> splittedData = new ArrayList<>();
    for (int i = 0; i < splits; i++) {
      byte[] cur = KryoWritableWrapper.<byte[]>unwrapIfNeeded(
          worker.getBroadcast(NEXT_WORKER_PIECES + "_part_" + i));
      splittedData.add(cur);
      totalLength += cur.length;
    }

    byte[] merged;
    if (splits == 1) {
      merged = splittedData.get(0);
    } else {
      merged = new byte[totalLength];
      int index = 0;
      for (int i = 0; i < splits; i++) {
        System.arraycopy(
            splittedData.get(i), 0, merged, index, splittedData.get(i).length);
        index += splittedData.get(i).length;
      }
    }

    KryoWritableWrapper<BlockWorkerPieces<S>> wrapper =
        new KryoWritableWrapper<>();
    WritableUtils.fromByteArrayUnsafe(
        merged, wrapper, new UnsafeReusableByteArrayInput());
    return wrapper.get();
  }
}
