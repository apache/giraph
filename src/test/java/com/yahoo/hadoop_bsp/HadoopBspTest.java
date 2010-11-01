package com.yahoo.hadoop_bsp;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class HadoopBspTest 
    extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public HadoopBspTest( String testName ) {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(HadoopBspTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        TestBSP<Integer, String, String> test = new TestBSP<Integer, String, String>();
        System.out.println(test.getSuperstep());
        assertTrue( true );
    }
}
