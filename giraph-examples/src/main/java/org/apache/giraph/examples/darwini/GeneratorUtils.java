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
package org.apache.giraph.examples.darwini;

import org.apache.giraph.conf.GiraphConfiguration;

import java.io.IOException;
import java.util.Random;

/**
 * This class holds all basic distributions required
 * to generate social graph.
 */
public class GeneratorUtils {

  /**
   * Distribtion loaded from the file
   */
  private static LoadFromFileDistributions DISTR;

  /**
   * Job configuration.
   */
  private final GiraphConfiguration conf;

  /**
   * Random number generator
   */
  private Random rnd = new Random();

  /**
   * Initialize generator with specified
   * configuration
   * @param conf generator configuration
   */
  public GeneratorUtils(GiraphConfiguration conf) {
    this.conf = conf;
    try {
      synchronized (GeneratorUtils.class) {
        if (DISTR == null) {
          DISTR = new LoadFromFileDistributions();
          DISTR.loadFromFile(Constants.FILE_TO_LOAD_FROM.get(conf));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Specifies a formula to balance joint degree
   * distribution during the random edge creation.
   * @param d1 source vertex degree
   * @param d2 target vertex degree
   * @return a probability of making an edge
   */
  public static double scoreDegreeDiff(double d1, double d2) {
    return Math.abs(d1 - d2) / (d1 + d2);
  }

  /**
   * Draw random degree from the distribution
   * @return random vertex degree
   */
  public int randomDegree() {
    return DISTR.randomDegree(rnd);
  }

  /**
   * In case of multi-stage graph generation we have to
   * omit some edges at first. So we omit edges based on binomial
   * distribution approach.
   * @param degree desired vertex degree
   * @return part of the vertex degree that needs to be satisfied
   * at subgraph generation stage
   */
  public int randomInSuperCommunityDegree(int degree) {
    //Binomial distribution with p = 0.16 of cross-country connection
    int res = 0;
    for (int i = 0; i < degree; i++) {
      if (rnd.nextDouble() < Constants.IN_COUNTRY_PERCENTAGE.get(conf)) {
        res++;
      }
    }
    return res;
  }

  /**
   * Draws random clustering coefficient using the
   * vertex degree
   * @param degree vertex degree
   * @return random clustering coefficient
   */
  public float randomCC(int degree) {
    return (float) DISTR.randomCC(rnd, degree);
  }

  /**
   * This is our way to group vertices with similar value
   * of cc * degree * (degree - 1). The formula here is
   * empiric but results don't differ much if you change it.
   * @param degree desired vertex degree
   * @param cc desired clustering coefficient
   * @return bucket hash code
   */
  public long hash(int degree, double cc) {
    return Math.round(Math.abs(1 +
        0.1 * rnd.nextGaussian()) *
        Math.pow(cc * degree * (degree - 1), 0.5));
  }

  /**
   * Pick random vertex id
   * @param total total number of vertices
   * @return random vertex id.
   */
  public long randomVertex(long total) {
    return (long) (rnd.nextDouble() * total);
  }
}
