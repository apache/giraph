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

import org.apache.giraph.jython.wrappers.JythonWritableWrapper;
import org.junit.Test;
import org.python.core.PyClass;
import org.python.core.PyFunction;
import org.python.core.PyInteger;
import org.python.core.PyMethod;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestJythonWritableWrapper {
  @Test
  public void testWrap() throws IOException {
    String jython =
        "class Foo:\n" +
        "    def __init__(self):\n" +
        "        self.val = 17\n" +
        "\n" +
        "    def do_add(self, x):\n" +
        "        self.val += x\n" +
        "\n" +
        "    def do_add_squared(self, x):\n" +
        "        self.do_add(x * x)\n" +
        "\n" +
        "    def new_other(self):\n" +
        "        self.other_val = 3\n" +
        "\n" +
        "def outside_add_squared(foo, x):\n" +
        "    foo.do_add_squared(x)\n" +
        "\n";

    PythonInterpreter interpreter = new PythonInterpreter();
    interpreter.exec(jython);

    PyObject fooClass = interpreter.get("Foo");
    assertTrue(fooClass instanceof PyClass);
    PyObject foo = fooClass.__call__();

    PyObject fooVal = foo.__getattr__("val");
    assertTrue(fooVal instanceof PyInteger);
    PyInteger val = (PyInteger) fooVal;
    assertEquals(17, val.getValue());

    PyObject function = interpreter.get("outside_add_squared");
    assertTrue("method class: " + function.getClass(), function instanceof PyFunction);
    function.__call__(foo, new PyInteger(3));

    fooVal = foo.__getattr__("val");
    assertTrue(fooVal instanceof PyInteger);
    val = (PyInteger) fooVal;
    assertEquals(26, val.getValue());

    JythonWritableWrapper wrappedFoo = new JythonWritableWrapper(foo);
    PyObject newOtherMethod = wrappedFoo.__getattr__("new_other");

    assertTrue(newOtherMethod instanceof PyMethod);
    newOtherMethod.__call__();

    function.__call__(wrappedFoo, new PyInteger(2));

    fooVal = foo.__getattr__("val");
    assertTrue(fooVal instanceof PyInteger);
    val = (PyInteger) fooVal;
    assertEquals(30, val.getValue());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    wrappedFoo.write(dos);

    byte[] data = baos.toByteArray();
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    DataInputStream dis = new DataInputStream(bais);

    PyObject foo2 = fooClass.__call__();

    PyObject foo2Val = foo2.__getattr__("val");
    assertTrue(foo2Val instanceof PyInteger);
    PyInteger val2 = (PyInteger) foo2Val;
    assertEquals(17, val2.getValue());

    JythonWritableWrapper wrappedFoo2 = new JythonWritableWrapper(foo2);

    foo2Val = wrappedFoo2.getPyObject().__getattr__("val");
    assertTrue(foo2Val instanceof PyInteger);
    val2 = (PyInteger) foo2Val;
    assertEquals(17, val2.getValue());

    wrappedFoo2.readFields(dis);

    foo2Val = wrappedFoo2.getPyObject().__getattr__("val");
    assertTrue(foo2Val instanceof PyInteger);
    val2 = (PyInteger) foo2Val;
    assertEquals(30, val2.getValue());
  }
}
