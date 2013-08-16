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
package org.apache.giraph.jython.wrappers;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;
import org.python.core.PyException;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.modules.cPickle;

import com.google.common.base.Preconditions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Wraps a Jython object, adding {@link WritableComparable} interface.
 * Used for graph types (IVEMM) that are in pure Jython.
 */
public class JythonWritableWrapper extends JythonWrapperBase
    implements WritableComparable {
  /** Logger */
  private static final Logger LOG =
      Logger.getLogger(JythonWritableWrapper.class);

  /**
   * Constructor
   *
   * @param pyObject jython object to wrap
   */
  public JythonWritableWrapper(PyObject pyObject) {
    super(pyObject);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    String str = WritableUtils.readString(in);
    PyString pyString = new PyString(str);
    Object object;
    try {
      object = cPickle.loads(pyString);
    } catch (PyException e) {
      LOG.fatal("Could not deserialize Jython value from string " + str);
      throw e;
    }
    Preconditions.checkArgument(object instanceof PyObject);
    setPyObject((PyObject) object);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    PyString pyString;
    try {
      pyString = cPickle.dumps(getPyObject());
    } catch (PyException e) {
      LOG.fatal("Could not serialize wrapped Jython value: " +
          getPyObject().__str__());
      throw e;
    }
    WritableUtils.writeString(out, pyString.getString());
  }

  @Override
  public int compareTo(Object other) {
    int result = -1;
    if (other instanceof PyObject) {
      PyObject pyOther = (PyObject) other;
      result = getPyObject().__cmp__(pyOther);
    } else if (JythonWritableWrapper.class.equals(other.getClass())) {
      JythonWritableWrapper wrapperOther = (JythonWritableWrapper) other;
      result = getPyObject().__cmp__(wrapperOther.getPyObject());
    }
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (obj instanceof JythonWritableWrapper) {
      return compareTo(obj) == 0;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return getPyObject().__hash__().asInt();
  }
}
