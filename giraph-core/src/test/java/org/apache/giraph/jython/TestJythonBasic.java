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

import org.junit.Test;
import org.python.core.PyClass;
import org.python.core.PyDictionary;
import org.python.core.PyInteger;
import org.python.core.PyList;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestJythonBasic {
  private static final double DELTA = 0.0000001;

  @Test
  public void testBasic() {
    String jython =
        "class Foo:\n" +
        "    def __init__(self):\n" +
        "        self.map = {\"32\": 32, \"4.3\": 4.3}\n" +
        "        self.list = [ 2, 9, 11 ]\n" +
        "        self.ival = 17\n" +
        "\n" +
        "def get_map(foo):\n" +
        "    return foo.map\n" +
        "\n" +
        "def get_list(foo):\n" +
        "    return foo.list\n" +
        "\n" +
        "def get_ival(foo):\n" +
        "    return foo.ival\n" +
        "";

    PythonInterpreter interpreter = new PythonInterpreter();
    interpreter.exec(jython);

    PyObject fooClass = interpreter.get("Foo");
    assertTrue(fooClass instanceof PyClass);

    PyObject getMapFunc = interpreter.get("get_map");
    PyObject getListFunc = interpreter.get("get_list");
    PyObject getIValFunc = interpreter.get("get_ival");

    PyObject foo = fooClass.__call__();

    PyObject mapResult = getMapFunc.__call__(foo);
    assertTrue(mapResult instanceof PyDictionary);
    PyDictionary pyMapResult = ((PyDictionary) mapResult);
    assertEquals(2, pyMapResult.size());
    Object thirtyTwo = pyMapResult.get("32");
    assertTrue(thirtyTwo instanceof Integer);
    assertEquals(32, ((Integer) thirtyTwo).intValue());
    Object fourPointThree = pyMapResult.get("4.3");
    assertTrue(fourPointThree instanceof Double);
    assertEquals(4.3, (Double) fourPointThree, DELTA);

    PyObject listResult = getListFunc.__call__(foo);
    assertTrue(listResult instanceof PyList);
    PyList pyListResult = (PyList) listResult;
    assertEquals(3, pyListResult.size());
    assertEquals(2, pyListResult.get(0));
    assertEquals(9, pyListResult.get(1));
    assertEquals(11, pyListResult.get(2));

    PyObject ivalResult = getIValFunc.__call__(foo);
    assertTrue(ivalResult instanceof PyInteger);
    assertEquals(17, ((PyInteger) ivalResult).getValue());
  }
}
