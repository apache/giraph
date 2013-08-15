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

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * User: ben
 * Date: 8/15/13  10:23 AM
 */
public class UnsafeByteArrayStreamTest {
    @Test
    public void simpleChars() throws IOException {
        String stringToTest = "Unsafe is synonomous with 'hard to debug'";

        UnsafeByteArrayOutputStream outputStream = new UnsafeByteArrayOutputStream();
        outputStream.writeUTF(stringToTest);
        outputStream.close();

        UnsafeByteArrayInputStream inputStream = new UnsafeByteArrayInputStream(outputStream.toByteArray());
        Assert.assertEquals(stringToTest, inputStream.readUTF());
    }

    @Test
    public void utf16Char() throws IOException {
        String stringToTest = "水 is a letter you dont see every day";

        UnsafeByteArrayOutputStream outputStream = new UnsafeByteArrayOutputStream();
        outputStream.writeUTF(stringToTest);
        outputStream.close();

        UnsafeByteArrayInputStream inputStream = new UnsafeByteArrayInputStream(outputStream.toByteArray());
        Assert.assertEquals(stringToTest, inputStream.readUTF());
    }

    @Test
    public void testRepetition() throws IOException {
        String stringToTest = "Speed outweighs complexity on most days";

        UnsafeByteArrayOutputStream outputStream = new UnsafeByteArrayOutputStream();

        for (int i = 0; i < 50; i++) {
            outputStream.writeUTF(stringToTest);
        }
        outputStream.close();

        UnsafeByteArrayInputStream inputStream = new UnsafeByteArrayInputStream(outputStream.toByteArray());

        for (int i = 0; i < 50; i++) {
            Assert.assertEquals(stringToTest, inputStream.readUTF());
        }

    }
}
