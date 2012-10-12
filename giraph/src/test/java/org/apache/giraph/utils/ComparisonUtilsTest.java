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

package org.apache.giraph.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.collect.Lists;

public class ComparisonUtilsTest {

    @Test
    public void testEquality() {
        Iterable<String> one = Lists.newArrayList("one", "two", "three");
        Iterable<String> two = Lists.newArrayList("one", "two", "three");

        assertTrue(ComparisonUtils.equal(one, one));
        assertTrue(ComparisonUtils.equal(one, two));
        assertTrue(ComparisonUtils.equal(two, two));
        assertTrue(ComparisonUtils.equal(two, one));
    }

    @Test
    public void testEqualityEmpty() {
        Iterable<String> one = Lists.newArrayList();
        Iterable<String> two = Lists.newArrayList();

        assertTrue(ComparisonUtils.equal(one, one));
        assertTrue(ComparisonUtils.equal(one, two));
        assertTrue(ComparisonUtils.equal(two, two));
        assertTrue(ComparisonUtils.equal(two, one));
    }

    @Test
    public void testInEquality() {
        Iterable<String> one = Lists.newArrayList("one", "two", "three");
        Iterable<String> two = Lists.newArrayList("two", "three", "four");
        Iterable<String> three = Lists.newArrayList();

        assertFalse(ComparisonUtils.equal(one, two));
        assertFalse(ComparisonUtils.equal(one, three));
        assertFalse(ComparisonUtils.equal(two, one));
        assertFalse(ComparisonUtils.equal(two, three));
        assertFalse(ComparisonUtils.equal(three, one));
        assertFalse(ComparisonUtils.equal(three, two));
    }

    @Test
    public void testInEqualityDifferentLengths() {
        Iterable<String> one = Lists.newArrayList("one", "two", "three");
        Iterable<String> two = Lists.newArrayList("one", "two", "three", "four");

        assertFalse(ComparisonUtils.equal(one, two));
        assertFalse(ComparisonUtils.equal(two, one));
    }

}
