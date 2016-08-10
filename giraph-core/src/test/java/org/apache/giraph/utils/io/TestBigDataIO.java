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

package org.apache.giraph.utils.io;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestBigDataIO {
  @Test
  public void testLargeByteArrays() throws IOException {
    ImmutableClassesGiraphConfiguration conf =
        new ImmutableClassesGiraphConfiguration(new Configuration());
    BigDataOutput output = new BigDataOutput(conf);
    byte[] byteArray1 = new byte[(1 << 28) + 3];
    int pos1 = (1 << 27) + 2;
    byteArray1[pos1] = 17;
    output.write(byteArray1);
    Assert.assertEquals(9, output.getNumberOfDataOutputs());
    byte[] byteArray2 = new byte[(1 << 27) - 1];
    int pos2 = (1 << 26) + 5;
    byteArray2[pos2] = 13;
    output.write(byteArray2);
    Assert.assertEquals(13, output.getNumberOfDataOutputs());

    BigDataInput in = new BigDataInput(output);
    byte[] byteArray3 = new byte[byteArray1.length];
    in.readFully(byteArray3);
    Assert.assertEquals(byteArray1[pos1], byteArray3[pos1]);
    byte[] byteArray4 = new byte[byteArray2.length];
    in.readFully(byteArray4);
    Assert.assertEquals(byteArray2[pos2], byteArray4[pos2]);
  }
}
