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

import org.apache.giraph.block_app.framework.internal.BlockMasterLogic;
import org.apache.giraph.block_app.framework.internal.BlockWorkerPieces;
import org.apache.giraph.block_app.framework.output.BlockOutputHandle;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.writable.kryo.KryoWritableWrapper;

/**
 * MasterCompute class which executes block computation.
 *
 * @param <S> Execution stage type
 */
public final class BlockMasterCompute<S> extends MasterCompute {
  private BlockMasterLogic<S> blockMasterLogic = new BlockMasterLogic<>();

  @Override
  public void initialize() throws InstantiationException,
      IllegalAccessException {
    blockMasterLogic.initialize(getConf(), new BlockMasterApiWrapper(this,
        new BlockOutputHandle(getContext().getJobID().toString(),
        getConf(), getContext())));
  }

  @Override
  public void compute() {
    BlockWorkerPieces<S> workerPieces =
        blockMasterLogic.computeNext(getSuperstep());
    if (workerPieces == null) {
      haltComputation();
    } else {
      BlockWorkerPieces.setNextWorkerPieces(this, workerPieces);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    new KryoWritableWrapper<>(blockMasterLogic).write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    KryoWritableWrapper<BlockMasterLogic<S>> object =
        new KryoWritableWrapper<>();
    object.readFields(in);
    blockMasterLogic = object.get();
    blockMasterLogic.initializeAfterRead(new BlockMasterApiWrapper(this,
        new BlockOutputHandle()));
  }
}
