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
