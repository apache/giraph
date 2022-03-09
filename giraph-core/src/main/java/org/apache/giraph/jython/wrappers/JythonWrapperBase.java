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

import org.python.core.PyObject;

import com.google.common.base.Objects;

// CHECKSTYLE: stop MethodNameCheck

/**
 * Base class for wrapping Jython objects
 */
public class JythonWrapperBase extends PyObject {
  /** The Jython object being wrapped */
  private PyObject pyObject;

  /**
   * Constructor
   *
   * @param pyObject Jython object to wrap
   */
  public JythonWrapperBase(PyObject pyObject) {
    this.pyObject = pyObject;
  }

  public PyObject getPyObject() {
    return pyObject;
  }

  public void setPyObject(PyObject pyObject) {
    this.pyObject = pyObject;
  }

  @Override
  public PyObject __findattr_ex__(String name) {
    return pyObject.__findattr_ex__(name);
  }

  @Override
  public void __setattr__(String name, PyObject value) {
    pyObject.__setattr__(name, value);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (obj instanceof JythonWrapperBase) {
      JythonWrapperBase other = (JythonWrapperBase) obj;
      return Objects.equal(pyObject, other.pyObject);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(pyObject);
  }
}

// CHECKSTYLE: resume MethodNameCheck
