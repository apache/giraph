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
package org.apache.giraph.block_app.framework.output;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.giraph.block_app.framework.api.BlockOutputApi;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.utils.CallableFactory;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;

/**
 * Handler for blocks output - keeps track of outputs and writers created
 */
@SuppressWarnings("unchecked")
public class BlockOutputHandle implements BlockOutputApi {
  private transient Configuration conf;
  private transient Progressable progressable;
  private final Map<String, BlockOutputDesc> outputDescMap;
  private final Map<String, Queue<BlockOutputWriter>> freeWriters =
      new HashMap<>();
  private final Map<String, Queue<BlockOutputWriter>> occupiedWriters =
      new HashMap<>();

  public BlockOutputHandle() {
    outputDescMap = null;
  }

  public BlockOutputHandle(String jobIdentifier, Configuration conf,
      Progressable hadoopProgressable) {
    outputDescMap = BlockOutputFormat.createInitAndCheckOutputDescsMap(
        conf, jobIdentifier);
    for (String confOption : outputDescMap.keySet()) {
      outputDescMap.get(confOption).preWriting();
      freeWriters.put(confOption,
          new ConcurrentLinkedQueue<BlockOutputWriter>());
      occupiedWriters.put(confOption,
          new ConcurrentLinkedQueue<BlockOutputWriter>());
    }
    initialize(conf, hadoopProgressable);
  }

  public void initialize(Configuration conf, Progressable progressable) {
    this.conf = conf;
    this.progressable = progressable;
  }


  @Override
  public <OW extends BlockOutputWriter, OD extends BlockOutputDesc<OW>>
  OD getOutputDesc(String confOption) {
    if (outputDescMap == null) {
      throw new IllegalArgumentException(
          "Output cannot be used with checkpointing");
    }
    return (OD) outputDescMap.get(confOption);
  }

  @Override
  public <OW extends BlockOutputWriter> OW getWriter(String confOption) {
    if (outputDescMap == null) {
      throw new IllegalArgumentException(
          "Output cannot be used with checkpointing");
    }
    OW outputWriter = (OW) freeWriters.get(confOption).poll();
    if (outputWriter == null) {
      outputWriter = (OW) outputDescMap.get(confOption).createOutputWriter(
          conf, progressable);
    }
    occupiedWriters.get(confOption).add(outputWriter);
    return outputWriter;
  }

  public void returnAllWriters() {
    for (Map.Entry<String, Queue<BlockOutputWriter>> entry :
        occupiedWriters.entrySet()) {
      freeWriters.get(entry.getKey()).addAll(entry.getValue());
      entry.getValue().clear();
    }
  }

  public void closeAllWriters() {
    final Queue<BlockOutputWriter> allWriters = new ConcurrentLinkedQueue<>();
    for (Queue<BlockOutputWriter> blockOutputWriters : freeWriters.values()) {
      allWriters.addAll(blockOutputWriters);
    }
    if (allWriters.isEmpty()) {
      return;
    }
    // Closing writers can take time - use multiple threads and call progress
    CallableFactory<Void> callableFactory = new CallableFactory<Void>() {
      @Override
      public Callable<Void> newCallable(int callableId) {
        return new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            BlockOutputWriter writer = allWriters.poll();
            while (writer != null) {
              writer.close();
              writer = allWriters.poll();
            }
            return null;
          }
        };
      }
    };
    ProgressableUtils.getResultsWithNCallables(callableFactory,
        Math.min(GiraphConstants.NUM_OUTPUT_THREADS.get(conf),
            allWriters.size()), "close-writers-%d", progressable);
    // Close all output formats
    for (BlockOutputDesc outputDesc : outputDescMap.values()) {
      outputDesc.postWriting();
    }
  }
}
