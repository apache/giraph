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
package org.apache.giraph.jython;

import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.GraphType;
import org.apache.giraph.graph.Language;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.jython.wrappers.JythonWritableWrapper;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.python.core.PyObject;

import com.google.common.base.Preconditions;

import java.io.IOException;

/**
 * The {@link org.apache.giraph.graph.Computation} class for using
 * Jython with Giraph. This class implements the Giraph necessary
 * interfaces but it actually holds a reference to the
 * {@link JythonComputation} which does the real work.
 *
 * The two classes are linked and together they allow us to coerce Jython types
 * to Writables.
 *
 * @param <I> Vertex ID
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M1> Incoming message value
 * @param <M2> Outgoing message value
 */
public class JythonGiraphComputation<I extends WritableComparable,
    V extends Writable, E extends Writable, M1 extends Writable,
    M2 extends Writable>
    extends AbstractComputation<I, V, E, M1, M2> {
  /** The user's computation class */
  private final JythonComputation jythonComputation;

  /**
   * Constructor
   *
   * @param jythonComputation the user's Jython computation
   */
  public JythonGiraphComputation(JythonComputation jythonComputation) {
    this.jythonComputation = jythonComputation;
  }

  @Override public void compute(Vertex<I, V, E> vertex, Iterable<M1> messages)
    throws IOException {
    jythonComputation.compute(vertex, messages);
  }

  /**
   * Wrap a vertex id in a {@link WritableComparable} wrapper if necessary
   *
   * @param object data to wrap
   * @return writable value
   */
  public WritableComparable wrapIdIfNecessary(Object object) {
    return wrapIfNecessary(object, GraphType.VERTEX_ID);
  }

  /**
   * Wrap a user value (IVEMM) in a {@link Writable} wrapper if necessary
   *
   * @param object data to wrap
   * @param graphType type of data (IVEMM)
   * @param <W> writable type
   * @return writable value
   */
  public <W extends Writable> W
  wrapIfNecessary(Object object, GraphType graphType) {
    if (graphType.interfaceClass().isInstance(object)) {
      return (W) object;
    }
    if (getConf().getValueLanguages().get(graphType) == Language.JYTHON &&
        getConf().getValueNeedsWrappers().get(graphType)) {
      Preconditions.checkArgument(object instanceof PyObject);
      return (W) new JythonWritableWrapper((PyObject) object);
    } else {
      return (W) object;
    }
  }
}
