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

package org.apache.giraph.examples;

import static org.apache.giraph.examples.BrachaTouegDeadlockComputation.BRACHA_TOUEG_DL_INITIATOR_ID;

import java.util.Iterator;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.examples.io.formats.BrachaTouegDeadlockInputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 *  Tests for {@link BrachaTouegDeadlockComputation}
 */
public class BrachaTouegDeadlockComputationTest {
  /** Giraph shared configuration */
  GiraphConfiguration conf = new GiraphConfiguration();
  
  @Before
  public void prepareConf() {
    this.conf.setComputationClass(BrachaTouegDeadlockComputation.class);
    this.conf.setVertexInputFormatClass(BrachaTouegDeadlockInputFormat.class);
    this.conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
  }

  @Test
  public void testEmptyGraph() throws Exception {
    Iterable<String>    results;
    Iterator<String>    result;
    String[]            graph = {};

    results = InternalVertexRunner.run(this.conf, graph);
    assert results != null;

    result = results.iterator();
    assert !result.hasNext();
  }

  @Test
  public void testOneInitNodeGraph() throws Exception {
    Iterable<String>    results;
    String[]            graph = { "[1,[]]" };
    String[]            expected = { "1\tisFree=true" };

    results = InternalVertexRunner.run(this.conf, graph);
    checkResults(results, expected);
  }

  @Test
  public void testOneNotInitNodeGraph() throws Exception {
    Iterable<String>    results;
    String[]            graph = { "[0,[]]" };
    String[]            expected = { "0\tisFree=false" };

    results = InternalVertexRunner.run(this.conf, graph);
    checkResults(results, expected);
  }

  @Test
  public void testThreeNodesAllNodesFreeGraph() throws Exception {
    Iterable<String>    results;
    String[]            graph = {
                                  "[1,[]]",
                                  "[2,[[1,0]]]",
                                  "[3,[[1,0]]]"
                                };
    String[]            expected = {
                                     "1\tisFree=true",
                                     "2\tisFree=false",
                                     "3\tisFree=false" 
                                   };

    results = InternalVertexRunner.run(this.conf, graph);
    checkResults(results, expected);
  }

  @Test
  public void testThreeNodesAllNodesBlockedGraph() throws Exception {
    Iterable<String>    results;
    String[]            graph = {
                                  "[1,[[2,0]]]",
                                  "[2,[[3,0]]]",
                                  "[3,[[1,0]]]"
                                };
    String[]            expected = {
                                     "1\tisFree=false",
                                     "2\tisFree=false",
                                     "3\tisFree=false" 
                                   };

    results = InternalVertexRunner.run(this.conf, graph);
    checkResults(results, expected);
  }

  @Test
  public void testThreeNodesAllNodesFreeMultiEdgesGraph() throws Exception {
    Iterable<String>    results;
    String[]            graph = {
                                  "[1,[[2,0],[3,1]]]",
                                  "[2,[]]",
                                  "[3,[]]"
                                };
    String[]            expected = {
                                     "1\tisFree=true",
                                     "2\tisFree=true",
                                     "3\tisFree=true" 
                                   };

    results = InternalVertexRunner.run(this.conf, graph);
    checkResults(results, expected);
  }

  @Test
  public void testThreeNodesAllNodesFreeNOutOfMGraph() throws Exception {
    Iterable<String>    results;
    String[]            graph = {
                                  "[1,[[2,0],[3,0]]]",
                                  "[2,[]]",
                                  "[3,[]]"
                                };
    String[]            expected = {
                                     "1\tisFree=true",
                                     "2\tisFree=true",
                                     "3\tisFree=true" 
                                   };

    results = InternalVertexRunner.run(this.conf, graph);
    checkResults(results, expected);
  }

  @Test
  public void testThreeNodesInitNodeFreeNOutOfMGraph() throws Exception {
    Iterable<String>    results;
    String[]            graph = {
                                  "[1,[[2,0],[3,0]]]",
                                  "[2,[]]",
                                  "[3,[]]"
                                };
    String[]            expected = {
                                     "1\tisFree=false",
                                     "2\tisFree=true",
                                     "3\tisFree=false" 
                                   };

    BRACHA_TOUEG_DL_INITIATOR_ID.set(this.conf, 2);
    results = InternalVertexRunner.run(this.conf, graph);
    checkResults(results, expected);
    BRACHA_TOUEG_DL_INITIATOR_ID.set(this.conf, 1);
  }

  @Test
  public void testThreeNodesAllNodesBlockedNOutOfMGraph() throws Exception {
    Iterable<String>    results;
    String[]            graph = {
                                  "[1,[[2,0],[3,0]]]",
                                  "[2,[[3,0]]]",
                                  "[3,[[2,0]]]"
                                };
    String[]            expected = {
                                     "1\tisFree=false",
                                     "2\tisFree=false",
                                     "3\tisFree=false" 
                                   };

    results = InternalVertexRunner.run(this.conf, graph);
    checkResults(results, expected);
  }

  @Test
  public void testFiveNodesAllNodesFreeNOutOfMGraph() throws Exception {
    Iterable<String>    results;
    String[]            graph = {
                                  "[1,[[2,0], [5,0]]]",
                                  "[2,[[4,0]]]",
                                  "[3,[[2,0], [5,0]]]",
                                  "[4,[[1,0], [5,1], [3,2]]]",
                                  "[5,[]]"
                                };
    String[]            expected = {
                                     "1\tisFree=true",
                                     "2\tisFree=true",
                                     "3\tisFree=true",
                                     "4\tisFree=true",
                                     "5\tisFree=true"
                                   };

    results = InternalVertexRunner.run(this.conf, graph);
    checkResults(results, expected);
  }

  @Test
  public void testFiveNodesAllNodesBlockedNOutOfMGraph() throws Exception {
    Iterable<String>    results;
    String[]            graph = {
                                  "[1,[[2,0], [5,0]]]",
                                  "[2,[[4,0]]]",
                                  "[3,[[2,0], [5,0]]]",
                                  "[4,[[1,0], [5,1], [3,1]]]",
                                  "[5,[]]"
                                };
    String[]            expected = {
                                     "1\tisFree=false",
                                     "2\tisFree=false",
                                     "3\tisFree=false",
                                     "4\tisFree=false",
                                     "5\tisFree=true"
                                   };

    results = InternalVertexRunner.run(this.conf, graph);
    checkResults(results, expected);
  }

  /**
   * Internal checker to verify the correctness of the tests.
   * @param results   the actual results obtaind
   * @param expected  expected results
   */
  private void checkResults(Iterable<String> results, String[] expected) {
    Iterator<String> result = results.iterator();

    assert results != null;

    while(result.hasNext()) {
      String  resultStr = result.next();
      boolean found = false;

      for (int j = 0; j < expected.length; ++j) {
        if (expected[j].equals(resultStr)) {
          found = true;
        }
      }

      assert found;
    }
  }
}
