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
package org.apache.giraph.block_app.framework.api.giraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.giraph.block_app.framework.internal.BlockWorkerContextLogic;
import org.apache.giraph.block_app.framework.internal.BlockWorkerPieces;
import org.apache.giraph.block_app.framework.output.BlockOutputHandle;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.writable.kryo.HadoopKryo;
import org.apache.giraph.writable.kryo.markers.KryoIgnoreWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * WorkerContext that executes receiver and sender blocks passed
 * into BlockWorkerPieces.
 */
public final class BlockWorkerContext extends WorkerContext
    implements KryoIgnoreWritable {
  public static final Logger LOG = Logger.getLogger(BlockWorkerContext.class);

  private BlockWorkerContextLogic workerLogic;

  @Override
  public void preApplication()
      throws InstantiationException, IllegalAccessException {
    workerLogic = new BlockWorkerContextLogic();
    workerLogic.preApplication(new BlockWorkerContextApiWrapper<>(this),
        new BlockOutputHandle(getContext().getJobID().toString(),
            getConf(), getContext()));
  }

  @Override
  public void preSuperstep() {
    List<Writable> messages = getAndClearMessagesFromOtherWorkers();
    BlockWorkerContextApiWrapper<WritableComparable, Writable> workerApi =
        new BlockWorkerContextApiWrapper<>(this);
    BlockWorkerPieces<Object> workerPieces =
        BlockWorkerPieces.getNextWorkerPieces(this);

    LOG.info("PassedComputation in " + getSuperstep() +
        " superstep executing " + workerPieces);

    workerLogic.preSuperstep(
        workerApi, workerApi, workerPieces, getSuperstep(), messages);
  }

  @Override
  public void postSuperstep() {
    workerLogic.postSuperstep();
  }

  @Override
  public void postApplication() {
    workerLogic.postApplication();
  }

  public Object getWorkerValue() {
    return workerLogic.getWorkerValue();
  }

  public BlockOutputHandle getOutputHandle() {
    return workerLogic.getOutputHandle();
  }

  // Cannot extend KryoWritable directly, since WorkerContext is
  // abstract class, not interface... Additionally conf in parent
  // class cannot be made transient.
  // So just add serialization of two individual fields.
  // (and adding KryoIgnoreWritable to avoid wrapping it twice)

  @Override
  public void write(DataOutput out) throws IOException {
    HadoopKryo.writeClassAndObj(out, workerLogic);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    workerLogic = HadoopKryo.readClassAndObj(in);
    workerLogic.getOutputHandle().initialize(getConf(), getContext());
  }
}
