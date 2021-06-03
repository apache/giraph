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

import java.io.Serializable;

import org.apache.giraph.function.Function;
import org.apache.giraph.function.primitive.Obj2LongFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.reflect.TypeToken;

public class TestLambdaConfOption {
  @Before
  public void setLogging() {
    Logger.getLogger(RuntimeClassGenerator.class).setLevel(Level.DEBUG);
  }

  @Test
  public void testLambdaConfOptionClass() throws NoSuchFieldException, SecurityException {
    Configuration conf = new Configuration();
    LambdaConfOption<ValueInIteration> testObjectFactoryConfOption =
        new LambdaConfOption<>("testFactory", ValueInIteration.class, "", "", "iter");

    testObjectFactoryConfOption.setCodeSnippet(conf, "");
    Assert.assertNull(testObjectFactoryConfOption.createObject(conf));

    testObjectFactoryConfOption.setCodeSnippet(conf, "iter + 5");
    ValueInIteration f = testObjectFactoryConfOption.createObject(conf);
    Assert.assertEquals(10, f.get(5), 0.0);
  }

  @Test
  public void testLambdaConfOptionTypeToken() throws NoSuchFieldException, SecurityException {
    Configuration conf = new Configuration();
    LambdaConfOption<Obj2LongFunction<LongWritable>> testObjectFactoryConfOption =
        new LambdaConfOption<>("testFactory", new TypeToken<Obj2LongFunction<LongWritable>>(){}, "", "", "id");

    testObjectFactoryConfOption.setCodeSnippet(conf, "");
    Assert.assertNull(testObjectFactoryConfOption.createObject(conf));

    testObjectFactoryConfOption.setCodeSnippet(conf, "id.get()");
    Obj2LongFunction<LongWritable> f = testObjectFactoryConfOption.createObject(conf);
    Assert.assertEquals(10, f.apply(new LongWritable(10)));
  }

  @Test
  public void testLambdaConfOptionTypeToken2() throws NoSuchFieldException, SecurityException {
    Configuration conf = new Configuration();
    LambdaConfOption<Function<LongWritable, DoubleWritable>> testObjectFactoryConfOption =
        new LambdaConfOption<>("testFactory", new TypeToken<Function<LongWritable, DoubleWritable>>(){}, "", "", "id");

    testObjectFactoryConfOption.setCodeSnippet(conf, "");
    Assert.assertNull(testObjectFactoryConfOption.createObject(conf));

    testObjectFactoryConfOption.setCodeSnippet(conf, "new DoubleWritable(id.get())");
    Function<LongWritable, DoubleWritable> f = testObjectFactoryConfOption.createObject(conf);
    Assert.assertEquals(10, f.apply(new LongWritable(10)).get(), 0.0);
  }

  /**
   * Value that depends on current iteration.
   */
  public interface ValueInIteration extends Serializable {
    float get(int iteration);
  }

  public static abstract class AbstractClass implements ValueInIteration {
    public static int get5() {
      return 5;
    }
  }

  @Test
  public void testLambdaConfOptionAbstractClass() throws NoSuchFieldException, SecurityException {
    Configuration conf = new Configuration();
    LambdaConfOption<AbstractClass> testObjectFactoryConfOption =
        new LambdaConfOption<>("testFactory", AbstractClass.class, "", "", "iter");

    testObjectFactoryConfOption.setCodeSnippet(conf, "");
    Assert.assertNull(testObjectFactoryConfOption.createObject(conf));

    testObjectFactoryConfOption.setCodeSnippet(conf, "iter + get5()");
    ValueInIteration f = testObjectFactoryConfOption.createObject(conf);
    Assert.assertEquals(10, f.get(5), 0.0);
  }
}
