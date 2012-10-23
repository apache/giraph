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

package org.apache.giraph.io.hcatalog;

import junit.framework.TestCase;

import java.util.Map;
import org.junit.Test;

public class TestHiveUtils extends TestCase {
  @Test
  public void testParsePartition() {
    String partitionStr = "feature1=2012-10-09, feature2=a1+b2, feature3=ff-gg";
    Map<String, String> partition = HiveUtils.parsePartitionValues(partitionStr);
    assertEquals(3, partition.size());
    assertEquals("2012-10-09", partition.get("feature1"));
    assertEquals("a1+b2", partition.get("feature2"));
    assertEquals("ff-gg", partition.get("feature3"));
  }
}
