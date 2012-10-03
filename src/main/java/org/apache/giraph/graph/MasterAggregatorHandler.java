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

package org.apache.giraph.graph;

import org.apache.giraph.bsp.SuperstepState;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import net.iharder.Base64;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

/** Master implementation of {@link AggregatorHandler} */
public class MasterAggregatorHandler extends AggregatorHandler implements
    MasterAggregatorUsage, Writable {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(MasterAggregatorHandler.class);
  /** Aggregator writer */
  private final AggregatorWriter aggregatorWriter;

  /**
   * @param config Hadoop configuration
   */
  public MasterAggregatorHandler(Configuration config) {
    aggregatorWriter = BspUtils.createAggregatorWriter(config);
  }

  @Override
  public <A extends Writable> void setAggregatedValue(String name, A value) {
    AggregatorWrapper<? extends Writable> aggregator = getAggregator(name);
    if (aggregator == null) {
      throw new IllegalStateException(
          "setAggregatedValue: Tried to set value of aggregator which wasn't" +
              " registered " + name);
    }
    ((AggregatorWrapper<A>) aggregator).setCurrentAggregatedValue(value);
  }

  @Override
  public <A extends Writable> boolean registerAggregator(String name,
      Class<? extends Aggregator<A>> aggregatorClass) throws
      InstantiationException, IllegalAccessException {
    return registerAggregator(name, aggregatorClass, false) != null;
  }

  @Override
  public <A extends Writable> boolean registerPersistentAggregator(String name,
      Class<? extends Aggregator<A>> aggregatorClass) throws
      InstantiationException, IllegalAccessException {
    return registerAggregator(name, aggregatorClass, true) != null;
  }

  /**
   * Get aggregator values supplied by workers for a particular superstep and
   * aggregate them
   *
   * @param superstep Superstep which we are preparing for
   * @param service BspService to get zookeeper info from
   */
  public void prepareSuperstep(long superstep, BspService service) {
    String workerFinishedPath =
        service.getWorkerFinishedPath(
            service.getApplicationAttempt(), superstep);
    List<String> hostnameIdPathList = null;
    try {
      hostnameIdPathList =
          service.getZkExt().getChildrenExt(
              workerFinishedPath, false, false, true);
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "collectAndProcessAggregatorValues: KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "collectAndProcessAggregatorValues: InterruptedException", e);
    }

    for (String hostnameIdPath : hostnameIdPathList) {
      JSONObject workerFinishedInfoObj = null;
      byte[] aggregatorArray = null;
      try {
        byte[] zkData =
            service.getZkExt().getData(hostnameIdPath, false, null);
        workerFinishedInfoObj = new JSONObject(new String(zkData));
      } catch (KeeperException e) {
        throw new IllegalStateException(
            "collectAndProcessAggregatorValues: KeeperException", e);
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            "collectAndProcessAggregatorValues: InterruptedException",
            e);
      } catch (JSONException e) {
        throw new IllegalStateException(
            "collectAndProcessAggregatorValues: JSONException", e);
      }
      try {
        aggregatorArray = Base64.decode(workerFinishedInfoObj.getString(
            service.JSONOBJ_AGGREGATOR_VALUE_ARRAY_KEY));
      } catch (JSONException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("collectAndProcessAggregatorValues: " +
              "No aggregators" + " for " + hostnameIdPath);
        }
        continue;
      } catch (IOException e) {
        throw new IllegalStateException(
            "collectAndProcessAggregatorValues: IOException", e);
      }

      DataInputStream input =
          new DataInputStream(new ByteArrayInputStream(aggregatorArray));
      try {
        while (input.available() > 0) {
          String aggregatorName = input.readUTF();
          AggregatorWrapper<Writable> aggregator =
              getAggregatorMap().get(aggregatorName);
          if (aggregator == null) {
            throw new IllegalStateException(
                "collectAndProcessAggregatorValues: " +
                    "Master received aggregator which isn't registered: " +
                    aggregatorName);
          }
          Writable aggregatorValue = aggregator.createInitialValue();
          aggregatorValue.readFields(input);
          aggregator.aggregateCurrent(aggregatorValue);
        }
      } catch (IOException e) {
        throw new IllegalStateException(
            "collectAndProcessAggregatorValues: " +
                "IOException when reading aggregator data", e);
      }
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("collectAndProcessAggregatorValues: Processed aggregators");
    }

    // prepare aggregators for master compute
    for (AggregatorWrapper<Writable> aggregator :
        getAggregatorMap().values()) {
      if (aggregator.isPersistent()) {
        aggregator.aggregateCurrent(aggregator.getPreviousAggregatedValue());
      }
      aggregator.setPreviousAggregatedValue(
          aggregator.getCurrentAggregatedValue());
      aggregator.resetCurrentAggregator();
    }
  }

  /**
   * Save the supplied aggregator values.
   *
   * @param superstep Superstep which we are finishing.
   * @param service BspService to get zookeeper info from
   */
  public void finishSuperstep(long superstep, BspService service) {
    Map<String, AggregatorWrapper<Writable>> aggregatorMap =
        getAggregatorMap();

    for (AggregatorWrapper<Writable> aggregator : aggregatorMap.values()) {
      if (aggregator.isChanged()) {
        // if master compute changed the value, use the one he chose
        aggregator.setPreviousAggregatedValue(
            aggregator.getCurrentAggregatedValue());
        // reset aggregator for the next superstep
        aggregator.resetCurrentAggregator();
      }
    }

    if (aggregatorMap.size() > 0) {
      String mergedAggregatorPath =
          service.getMergedAggregatorPath(
              service.getApplicationAttempt(),
              superstep);

      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      DataOutput output = new DataOutputStream(outputStream);
      try {
        output.writeInt(aggregatorMap.size());
      } catch (IOException e) {
        e.printStackTrace();
      }
      for (Map.Entry<String, AggregatorWrapper<Writable>> entry :
          aggregatorMap.entrySet()) {
        try {
          output.writeUTF(entry.getKey());
          output.writeUTF(entry.getValue().getAggregatorClass().getName());
          entry.getValue().getPreviousAggregatedValue().write(output);
        } catch (IOException e) {
          throw new IllegalStateException("saveAggregatorValues: " +
              "IllegalStateException", e);
        }
      }

      try {
        service.getZkExt().createExt(mergedAggregatorPath,
            outputStream.toByteArray(),
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT,
            true);
      } catch (KeeperException.NodeExistsException e) {
        LOG.warn("saveAggregatorValues: " +
            mergedAggregatorPath + " already exists!");
      } catch (KeeperException e) {
        throw new IllegalStateException(
            "saveAggregatorValues: KeeperException", e);
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            "saveAggregatorValues: IllegalStateException",
            e);
      }
      if (LOG.isInfoEnabled()) {
        LOG.info("saveAggregatorValues: Finished loading " +
            mergedAggregatorPath);
      }
    }
  }

  /**
   * Write aggregators to {@link AggregatorWriter}
   *
   * @param superstep Superstep which just finished
   * @param superstepState State of the superstep which just finished
   */
  public void writeAggregators(long superstep,
      SuperstepState superstepState) {
    try {
      Iterable<Map.Entry<String, Writable>> iter =
          Iterables.transform(
              getAggregatorMap().entrySet(),
              new Function<Map.Entry<String, AggregatorWrapper<Writable>>,
                  Map.Entry<String, Writable>>() {
                @Override
                public Map.Entry<String, Writable> apply(
                    Map.Entry<String, AggregatorWrapper<Writable>> entry) {
                  return new AbstractMap.SimpleEntry<String,
                      Writable>(entry.getKey(),
                      entry.getValue().getPreviousAggregatedValue());
                }
              });
      aggregatorWriter.writeAggregator(iter,
          (superstepState == SuperstepState.ALL_SUPERSTEPS_DONE) ?
              AggregatorWriter.LAST_SUPERSTEP : superstep);
    } catch (IOException e) {
      throw new IllegalStateException(
          "coordinateSuperstep: IOException while " +
              "writing aggregators data", e);
    }
  }

  /**
   * Initialize {@link AggregatorWriter}
   *
   * @param service BspService
   */
  public void initialize(BspService service) {
    try {
      aggregatorWriter.initialize(service.getContext(),
          service.getApplicationAttempt());
    } catch (IOException e) {
      throw new IllegalStateException("MasterAggregatorHandler: " +
          "Couldn't initialize aggregatorWriter", e);
    }
  }

  /**
   * Close {@link AggregatorWriter}
   *
   * @throws IOException
   */
  public void close() throws IOException {
    aggregatorWriter.close();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Map<String, AggregatorWrapper<Writable>> aggregatorMap =
        getAggregatorMap();
    out.writeInt(aggregatorMap.size());
    for (Map.Entry<String, AggregatorWrapper<Writable>> entry :
        aggregatorMap.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeUTF(entry.getValue().getAggregatorClass().getName());
      out.writeBoolean(entry.getValue().isPersistent());
      entry.getValue().getPreviousAggregatedValue().write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    Map<String, AggregatorWrapper<Writable>> aggregatorMap =
        getAggregatorMap();
    aggregatorMap.clear();
    int numAggregators = in.readInt();
    for (int i = 0; i < numAggregators; i++) {
      String aggregatorName = in.readUTF();
      String aggregatorClassName = in.readUTF();
      boolean isPersistent = in.readBoolean();
      AggregatorWrapper<Writable> aggregator =
          registerAggregator(aggregatorName, aggregatorClassName,
              isPersistent);
      Writable value = aggregator.createInitialValue();
      value.readFields(in);
      aggregator.setPreviousAggregatedValue(value);
    }
  }
}
