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
package org.apache.giraph.writable.kryo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.io.Writable;

/**
 * Generic wrapper object, making any object writable.
 *
 * Usage of this class is similar to KryoWritableWrapper but
 * unlike KryoWritableWrapper, this class does not
 * support recursive/nested objects to provide better
 * performance.
 *
 * If the underlying stream is a kryo output stream than the read/write
 * happens with a kryo object that doesn't track references, providing
 * significantly better performance.
 *
 * @param <T> Object type
 */
public class KryoSimpleWrapper<T> implements Writable, Boxed<T> {

  /** Wrapped object */
  private T object;

  /**
   * Create wrapper given an object.
   * @param object Object instance
   */
  public KryoSimpleWrapper(T object) {
    this.object = object;
  }

  /**
   * Creates wrapper initialized with null.
   */
  public KryoSimpleWrapper() {
  }

  /**
   * Unwrap the object value
   * @return Object value
   */
  public T get() {
    return object;
  }

  /**
   * Set wrapped object value
   * @param object New object value
   */
  public void set(T object) {
    this.object = object;
  }

  @Override
  public void readFields(DataInput in) throws java.io.IOException {
    if (in instanceof Input) {
      Input inp = (Input) in;
      object = HadoopKryo.readWithKryo(HadoopKryo.getNontrackingKryo(), inp);
    } else {
      object = HadoopKryo.readClassAndObj(in);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (out instanceof Output) {
      Output outp = (Output) out;
      HadoopKryo.writeWithKryo(HadoopKryo.getNontrackingKryo(), outp, object);
    } else {
      HadoopKryo.writeClassAndObj(out, object);
    }
  }
}
