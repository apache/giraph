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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.giraph.block_app.framework.piece.AbstractPiece;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterables;

/**
 * Tests repeatBlock's correctness
 */
public class TestRepeatBlock {

  public static final int REPEAT_TIMES = 5;

  @Test
  public void testRepeatBlockBasic() throws Exception {
    Piece piece1 = new Piece();
    Piece piece2 = new Piece();
    Block innerBlock = new SequenceBlock(piece1, piece2);
    Block repeatBlock = new RepeatBlock(
            REPEAT_TIMES,
            innerBlock
    );
    BlockTestingUtils.testIndependence(
            Iterables.concat(Collections.nCopies(REPEAT_TIMES, Arrays.asList(piece1, piece2))),
            repeatBlock);
    Assert.assertEquals(REPEAT_TIMES * 2, repeatBlock.getPieceCount().getCount());
  }

  @Test
  public void testNestedRepeatBlock() throws Exception {
    Piece piece1 = new Piece();
    Piece piece2 = new Piece();
    Block innerBlock = new SequenceBlock(piece1, piece2);
    Block repeatBlock = new RepeatBlock(
            REPEAT_TIMES,
            innerBlock
    );
    BlockTestingUtils.testNestedRepeatBlock(
            Iterables.concat(Collections.nCopies(REPEAT_TIMES, Arrays.asList(piece1, piece2))),
            repeatBlock);
  }

  @Test
  public void testRepeatBlockEmpty() throws Exception {
    Block innerBlock = new EmptyBlock();
    Block repeatBlock = new RepeatBlock(
            REPEAT_TIMES,
            innerBlock
    );
    List<? extends AbstractPiece> referenceImpl = Collections.emptyList();
    BlockTestingUtils.testIndependence(
            // Concatenating EmptyIterator = just EmptyIterator. No obj's to
            // compare against either
            referenceImpl,
            repeatBlock);
  }

}
