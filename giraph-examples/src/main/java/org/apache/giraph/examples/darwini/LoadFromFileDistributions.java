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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * Load joint degree - clustering coefficient
 * distribution from file. File must be in the
 * CSV format with 3 columns:
 * degree, clustering coefficient, number of vertices
 */
public class LoadFromFileDistributions {

  /**
   * Generators for degree distributions.
   */
  private CummulativeProbabilitiesGenerator degreeGenerator =
      new CummulativeProbabilitiesGenerator();
  /**
   * Generators for local clustering coefficient distributions.
   */
  private Int2ObjectMap<CummulativeProbabilitiesGenerator> ccGenerators =
      new Int2ObjectOpenHashMap<>();

  /**
   * Load from the specified file.
   * @param path file to load distribution from.
   * @throws IOException
   */
  public void loadFromFile(String path) throws IOException {
    InputStream is = getClass().getClassLoader().getResourceAsStream(path);
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(is, StandardCharsets.UTF_8));
    String line;
    while ((line = reader.readLine()) != null) {
      if (line.length() > 0 && line.charAt(0) >= '0' && line.charAt(0) <= '9') {
        String[] fields = line.split(",");
        int degree = Integer.parseInt(fields[0]);
        double cc = Double.parseDouble(fields[1]);
        int count = Integer.parseInt(fields[2]);
        degreeGenerator.add(degree, count);
        CummulativeProbabilitiesGenerator ccGenerator =
            ccGenerators.get(degree);
        if (ccGenerator == null) {
          ccGenerator = new CummulativeProbabilitiesGenerator();
          ccGenerators.put(degree, ccGenerator);
        }
        ccGenerator.add(cc, count);
      }
    }
    reader.close();
  }

  /**
   * Samples random degree using the loaded distribution.
   *
   * @param rnd random number generator
   * @return random degree
   */
  public int randomDegree(Random rnd) {
    return (int) degreeGenerator.draw(rnd);
  }

  /**
   * Samples random clustering coefficient using the
   * loaded distribution.
   * @param rnd random number generator
   * @param degree vertex degree
   * @return random clustering coefficient
   */
  public double randomCC(Random rnd, int degree) {
    return ccGenerators.get(degree).draw(rnd);
  }
}
