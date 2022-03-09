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

package org.apache.giraph.mapping.translate;

import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.worker.BspServiceWorker;
import org.apache.giraph.worker.LocalData;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

/**
 * Basic implementation of Translate Edge
 * where I = LongWritable &amp; B = ByteWritable
 *
 * @param <E> edge value type
 */
@SuppressWarnings("unchecked")
public class LongByteTranslateEdge<E extends Writable>
  extends DefaultImmutableClassesGiraphConfigurable
  implements TranslateEdge<LongWritable, E> {

  /** Local data used for targetId translation of edge */
  private LocalData<LongWritable,
    ? extends Writable, E, ByteWritable> localData;

  @Override
  public void initialize(BspServiceWorker<LongWritable,
    ? extends Writable, E> service) {
    localData = (LocalData<LongWritable, ? extends Writable, E, ByteWritable>)
      service.getLocalData();
  }

  @Override
  public LongWritable translateId(LongWritable targetId) {
    LongWritable translatedId = new LongWritable();
    translatedId.set(targetId.get());
    localData.getMappingStoreOps().embedTargetInfo(translatedId);
    return translatedId;
  }

  @Override
  public E cloneValue(E edgeValue) {
    // If vertex input does not have create edges,
    // then you can use LongByteTranslateEdge directly
    throw new UnsupportedOperationException();
  }

  /**
   * Correct implementation of cloneValue when edgevalue = nullwritable
   */
  public static class NoEdgeValue
    extends LongByteTranslateEdge<NullWritable> {
    @Override
    public NullWritable cloneValue(NullWritable edgeValue) {
      return NullWritable.get();
    }
  }

  /**
   * Correct implementation of cloneValue when edgevalue = intwritable
   */
  public static class IntEdgeValue
      extends LongByteTranslateEdge<IntWritable> {
    @Override
    public IntWritable cloneValue(IntWritable edgeValue) {
      return new IntWritable(edgeValue.get());
    }
  }

  /**
   * Correct implementation of cloneValue when edgevalue = longwritable
   */
  public static class LongEdgeValue
    extends LongByteTranslateEdge<LongWritable> {
    @Override
    public LongWritable cloneValue(LongWritable edgeValue) {
      return new LongWritable(edgeValue.get());
    }
  }

  /**
   * Correct implementation of cloneValue when edgevalue = floatwritable
   */
  public static class FloatEdgeValue
    extends LongByteTranslateEdge<FloatWritable> {
    @Override
    public FloatWritable cloneValue(FloatWritable edgeValue) {
      return new FloatWritable(edgeValue.get());
    }
  }

  /**
   * Correct implementation of cloneValue when edgevalue = doublewritable
   */
  public static class DoubleEdgeValue
    extends LongByteTranslateEdge<DoubleWritable> {
    @Override
    public DoubleWritable cloneValue(DoubleWritable edgeValue) {
      return new DoubleWritable(edgeValue.get());
    }
  }
}
