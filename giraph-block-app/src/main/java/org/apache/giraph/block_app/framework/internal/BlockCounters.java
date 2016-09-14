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

import java.lang.reflect.Field;

import org.apache.giraph.block_app.framework.api.Counter;
import org.apache.giraph.block_app.framework.api.StatusReporter;
import org.apache.giraph.block_app.framework.internal.BlockMasterLogic.TimeStatsPerEvent;

import org.apache.hadoop.mapreduce.Mapper;

/** Utility class for Blocks Framework related counters */
public class BlockCounters {
  public static final String GROUP = "Blocks Framework";

  private BlockCounters() { }

  /**
   * Takes all fields from stage object, and puts them into counters,
   * if possible.
   * Only fields that are convertible to long via widening are set
   * (i.e. long/int/short/byte)
   */
  public static void setStageCounters(
      String prefix, Object stage, StatusReporter reporter) {
    if (stage != null && reporter != null) {
      Class<?> clazz = stage.getClass();

      while (clazz != null) {
        Field[] fields = clazz.getDeclaredFields();

        Field.setAccessible(fields, true);
        for (Field field : fields) {
          try {
            long value = field.getLong(stage);
            reporter.getCounter(
                GROUP, prefix + field.getName()).setValue(value);

          // CHECKSTYLE: stop EmptyBlock - ignore any exceptions
          } catch (IllegalArgumentException | IllegalAccessException e) {
          }
          // CHECKSTYLE: resume EmptyBlock
        }
        clazz = clazz.getSuperclass();
      }
    }
  }

  public static void setMasterTimeCounter(
      PairedPieceAndStage<?> masterPiece, long superstep,
      long millis, StatusReporter reporter,
      TimeStatsPerEvent timeStats) {
    String name = masterPiece.getPiece().toString();
    reporter.getCounter(
        GROUP + " Master Timers",
        String.format(
            "In %6.1f %s (s)", superstep - 0.5, name)
    ).setValue(millis / 1000);
    timeStats.inc(name, millis);
  }

  public static void setWorkerTimeCounter(
      BlockWorkerPieces<?> workerPieces, long superstep,
      long millis, StatusReporter reporter,
      TimeStatsPerEvent timeStats) {
    String name = workerPieces.toStringShort();
    reporter.getCounter(
        GROUP + " Worker Timers",
        String.format("In %6d %s (s)", superstep, name)
    ).setValue(millis / 1000);
    timeStats.inc(name, millis);
  }

  public static Counter getCounter(
      Mapper.Context context, String group, String name) {
    final org.apache.hadoop.mapreduce.Counter counter =
        context.getCounter(group, name);
    return new Counter() {
      @Override
      public void increment(long incr) {
        counter.increment(incr);
      }

      @Override
      public void setValue(long value) {
        counter.setValue(value);
      }
    };
  }

  public static Counter getNoOpCounter() {
    return new Counter() {
      @Override
      public void setValue(long value) { }

      @Override
      public void increment(long incr) { }
    };
  }
}
