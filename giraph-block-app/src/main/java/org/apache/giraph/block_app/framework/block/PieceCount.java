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

package org.apache.giraph.block_app.framework.block;

import com.google.common.base.Objects;

/**
 * Number of pieces
 */
public class PieceCount {
  private boolean known;
  private int count;

  public PieceCount(int count) {
    known = true;
    this.count = count;
  }

  private PieceCount() {
    known = false;
  }

  public static PieceCount createUnknownCount() {
    return new PieceCount();
  }


  public PieceCount add(PieceCount other) {
    if (!this.known || !other.known) {
      known = false;
    } else {
      count += other.count;
    }
    return this;
  }

  public PieceCount multiply(int value) {
    count *= value;
    return this;
  }

  public int getCount() {
    if (known) {
      return count;
    } else {
      throw new IllegalStateException(
          "Can't get superstep count when it's unknown");
    }
  }

  public boolean isKnown() {
    return known;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof PieceCount) {
      PieceCount other = (PieceCount) obj;
      if (known) {
        return other.known && other.count == count;
      } else {
        return !other.known;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(known, count);
  }
}
