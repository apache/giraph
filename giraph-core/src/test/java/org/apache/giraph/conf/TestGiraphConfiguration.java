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

package org.apache.giraph.conf;

import org.junit.Test;

import static org.apache.giraph.conf.ClassConfOption.getClassesOfType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestGiraphConfiguration {
  public interface If { }
  public class A implements If { }
  public class B implements If { }
  public class C implements If { }

  @Test
  public void testSetClasses() {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setClasses("foo", If.class, A.class, B.class);
    Class<?>[] klasses = conf.getClasses("foo");
    assertEquals(2, klasses.length);
    assertEquals(A.class, klasses[0]);
    assertEquals(B.class, klasses[1]);

    try {
      conf.setClasses("foo", A.class, B.class);
      fail();
    } catch (RuntimeException e) {
      assertEquals(2, conf.getClasses("foo").length);
    }

    Class<? extends If>[] klasses2 = getClassesOfType(conf, "foo", If.class);
    assertEquals(2, klasses2.length);
    assertEquals(A.class, klasses2[0]);
    assertEquals(B.class, klasses2[1]);
  }

  @Test
  public void testAddToClasses() {
    GiraphConfiguration conf = new GiraphConfiguration();

    conf.setClasses("foo", If.class, A.class, B.class);
    ClassConfOption.addToClasses(conf, "foo", C.class, If.class);
    Class<?>[] klasses = conf.getClasses("foo");
    assertEquals(3, klasses.length);
    assertEquals(A.class, klasses[0]);
    assertEquals(B.class, klasses[1]);
    assertEquals(C.class, klasses[2]);

    ClassConfOption.addToClasses(conf, "bar", B.class, If.class);
    klasses = conf.getClasses("bar");
    assertEquals(1, klasses.length);
    assertEquals(B.class, klasses[0]);
  }
}
