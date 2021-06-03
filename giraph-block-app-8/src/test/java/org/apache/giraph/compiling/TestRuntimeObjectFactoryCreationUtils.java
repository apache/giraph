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
package org.apache.giraph.compiling;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestRuntimeObjectFactoryCreationUtils {
  @Before
  public void setLogging() {
    Logger.getLogger(RuntimeClassGenerator.class).setLevel(Level.DEBUG);
  }

  @Test
  public void testRuntimeObjectFactoryCreationUtils() {
    int startCount = RuntimeObjectFactoryGenerator.getCreatedClassesCount();

    TestClass1 testClass1Object = RuntimeObjectFactoryGenerator.createFactory(
        "return new TestClass1(1, \"A\", 0.5);", TestClass1.class).create();
    Assert.assertEquals(testClass1Object.x, 1);
    Assert.assertEquals(testClass1Object.y, "A");
    Assert.assertEquals(testClass1Object.z, 0.5, 0);
    Assert.assertEquals(startCount + 1,
        RuntimeObjectFactoryGenerator.getCreatedClassesCount());

    TestClass2 testClass2Object = RuntimeObjectFactoryGenerator.createFactory(
        "return new TestClass2(5);", TestClass2.class).create();
    Assert.assertEquals(testClass2Object.a, 5);
    Assert.assertEquals(startCount + 2,
        RuntimeObjectFactoryGenerator.getCreatedClassesCount());

    testClass1Object = RuntimeObjectFactoryGenerator.createFactory(
        "return new TestClass1(5, \"XY\", 1.5).setZ(3.5);", TestClass1.class).create();
    Assert.assertEquals(testClass1Object.x, 5);
    Assert.assertEquals(testClass1Object.y, "XY");
    Assert.assertEquals(testClass1Object.z, 3.5, 0);
    Assert.assertEquals(startCount + 3,
        RuntimeObjectFactoryGenerator.getCreatedClassesCount());

    testClass1Object = RuntimeObjectFactoryGenerator.createFactory(
        "return new TestClass1(1, \"A\", 0.5);", TestClass1.class).create();
    Assert.assertEquals(testClass1Object.x, 1);
    Assert.assertEquals(testClass1Object.y, "A");
    Assert.assertEquals(testClass1Object.z, 0.5, 0);
    Assert.assertEquals(startCount + 3,
        RuntimeObjectFactoryGenerator.getCreatedClassesCount());

    testClass1Object = RuntimeObjectFactoryGenerator.createFactory(
        "TestClass1 ret = new TestClass1(5, \"XY\", 1.5); ret.setZ(3.5); return ret;",
        TestClass1.class).create();
    Assert.assertEquals(testClass1Object.x, 5);
    Assert.assertEquals(testClass1Object.y, "XY");
    Assert.assertEquals(testClass1Object.z, 3.5, 0);
    Assert.assertEquals(startCount + 4,
        RuntimeObjectFactoryGenerator.getCreatedClassesCount());

    testClass1Object = RuntimeObjectFactoryGenerator.createFactory(
        "return new TestClass1() {{ x=1;y=\"A\";z=0.5; }};",
        TestClass1.class).create();
    Assert.assertEquals(testClass1Object.x, 1);
    Assert.assertEquals(testClass1Object.y, "A");
    Assert.assertEquals(testClass1Object.z, 0.5, 0);
    Assert.assertEquals(startCount + 5,
        RuntimeObjectFactoryGenerator.getCreatedClassesCount());
  }

  @Test
  public void testObjectConfOption() {
    Configuration conf = new Configuration();
    ObjectFactoryConfOption<TestClass1> testObjectFactoryConfOption = new ObjectFactoryConfOption<>(
        "testFactory", TestClass1.class, "return new TestClass1(50, \"XY\", 1.5);", "");
    TestClass1 testClass1Object = testObjectFactoryConfOption.createObject(conf);
    Assert.assertEquals(testClass1Object.x, 50);
    Assert.assertEquals(testClass1Object.y, "XY");
    Assert.assertEquals(testClass1Object.z, 1.5, 0);

    testObjectFactoryConfOption.setCodeSnippet(conf, "");
    Assert.assertNull(testObjectFactoryConfOption.createObject(conf));

    testObjectFactoryConfOption.setCodeSnippet(conf, "return new TestClass1(10, \"A\", 0.5);");
    testClass1Object = testObjectFactoryConfOption.createObject(conf);
    Assert.assertEquals(testClass1Object.x, 10);
    Assert.assertEquals(testClass1Object.y, "A");
    Assert.assertEquals(testClass1Object.z, 0.5, 0);

    TestClass1 testClass1OtherObject = testObjectFactoryConfOption.createObject(conf);
    Assert.assertEquals(testClass1OtherObject.x, 10);
    Assert.assertEquals(testClass1OtherObject.y, "A");
    Assert.assertEquals(testClass1OtherObject.z, 0.5, 0);

    // Verify that createObject returns different instance
    testClass1OtherObject.setZ(5);
    Assert.assertEquals(testClass1OtherObject.z, 5, 0);
    Assert.assertEquals(testClass1Object.z, 0.5, 0);

    ObjectInitializerConfOption<TestClass1> testObjectInitializerConfOption =
        new ObjectInitializerConfOption<>("testInitializer", TestClass1.class,
            "x=3;y=\"Y\";z=3.5;", "");
    testClass1Object = testObjectInitializerConfOption.createObject(conf);
    Assert.assertEquals(testClass1Object.x, 3);
    Assert.assertEquals(testClass1Object.y, "Y");
    Assert.assertEquals(testClass1Object.z, 3.5, 0);
  }

  public static class TestClass1 {
    protected int x;
    protected String y;
    protected double z;

    public TestClass1(int x, String y, double z) {
      this.x = x;
      this.y = y;
      this.z = z;
    }

    public TestClass1() {
    }

    public TestClass1 setZ(double z) {
      this.z = z;
      return this;
    }
  }

  public static class TestClass2 {
    private final int a;

    public TestClass2(int a) {
      this.a = a;
    }
  }
}
