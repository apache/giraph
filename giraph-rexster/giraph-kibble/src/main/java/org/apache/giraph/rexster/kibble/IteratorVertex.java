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

package org.apache.giraph.rexster.kibble;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

class IteratorVertex implements Iterator<Element> {
  private Iterator<Vertex> vertices = null;
  private long start;
  private long end;
  private long counter;

  public IteratorVertex(Iterator<Vertex> vertices, long start, long end) {
    this.vertices = vertices;
    this.start = start;
    this.end = end;
    this.counter = 0;
  }

  @Override
  public boolean hasNext() {
    if (counter >= this.end) {
      return false;
    }
    return vertices.hasNext();
  }

  @Override
  public Element next() {
    while (counter < this.start) {
      vertices.next();
      counter++;
    }

    if (counter >= this.start && counter < this.end) {
      return vertices.next();
    }

    throw new NoSuchElementException();
  }

  @Override
  public void remove() {
  }
}
