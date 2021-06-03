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

import org.apache.giraph.writable.kryo.markers.KryoIgnoreWritable;

/**
 * Class which you can extend to get all serialization/deserialization
 * done automagically
 */
public abstract class KryoWritable implements KryoIgnoreWritable {
  @Override
  public final void write(DataOutput out) throws IOException {
    HadoopKryo.writeOutOfObject(out, this);
  }

  @Override
  public final void readFields(DataInput in) throws IOException {
    HadoopKryo.readIntoObject(in, this);
  }
}
