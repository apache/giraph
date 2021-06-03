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
import java.io.UTFDataFormatException;

import static org.junit.Assert.assertEquals;

public class TestUnsafeByteArrayOutputStream {
    @Test
    public void testWriteBytes() throws IOException {
        UnsafeByteArrayOutputStream os = new UnsafeByteArrayOutputStream();
        int length = os.getByteArray().length;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append("\u00ea");
        }

        String s = sb.toString();
        os.writeBytes(s);

        UnsafeByteArrayInputStream is = new UnsafeByteArrayInputStream(os.getByteArray());

        for (int i = 0; i < s.length(); i++) {
            assertEquals((byte) s.charAt(i), is.readByte());
        }

        os.close();
    }

    @Test
    public void testWriteChars() throws IOException {
        UnsafeByteArrayOutputStream os = new UnsafeByteArrayOutputStream();
        int length = os.getByteArray().length;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append("\u10ea");
        }

        String s = sb.toString();
        os.writeChars(s);

        UnsafeByteArrayInputStream is = new UnsafeByteArrayInputStream(os.getByteArray());

        for (int i = 0; i < s.length(); i++) {
            assertEquals(s.charAt(i), is.readChar());
        }

        os.close();
    }

    @Test
    public void testWriteUTF() throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            sb.append("\u06ea");
        }

        String s = sb.toString();

        assertEquals(s, writeAndReadUTF(s));
    }

    @Test
    public void testWriteLongUTF() throws IOException {
        int maxLength = 65535;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < maxLength; i++) {
            sb.append("a");
        }

        String s = sb.toString();

        assertEquals(s, writeAndReadUTF(s));

        s = sb.append("a").toString();
        try {
            writeAndReadUTF(s);
            throw new IllegalStateException("Exception expected");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof UTFDataFormatException);
        }
    }

    private String writeAndReadUTF(String s) throws IOException {
        UnsafeByteArrayOutputStream os = new UnsafeByteArrayOutputStream();
        os.writeUTF(s);
        os.close();
        UnsafeByteArrayInputStream is = new UnsafeByteArrayInputStream(os.getByteArray());
        return is.readUTF();
    }
}
