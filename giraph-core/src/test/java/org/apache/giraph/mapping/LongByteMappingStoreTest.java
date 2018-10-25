package org.apache.giraph.mapping;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class LongByteMappingStoreTest {

  @Test
  public void test() {
    ImmutableClassesGiraphConfiguration conf =
      new ImmutableClassesGiraphConfiguration(new GiraphConfiguration());
    GiraphConstants.LB_MAPPINGSTORE_UPPER.setIfUnset(conf, 100);
    GiraphConstants.LB_MAPPINGSTORE_LOWER.setIfUnset(conf, 4);
    LongByteMappingStore store = new LongByteMappingStore();
    store.setConf(conf);
    store.initialize();
    store.addEntry(new LongWritable(1), new ByteWritable((byte) 1));
    store.addEntry(new LongWritable(2), new ByteWritable((byte) 2));
    store.postFilling();

    assertEquals((byte) 1, store.getByteTarget(new LongWritable(1)));
    assertEquals((byte) 2, store.getByteTarget(new LongWritable(2)));
    assertEquals((byte) -1, store.getByteTarget(new LongWritable(999)));

    assertEquals(new ByteWritable((byte) 1),
      store.getTarget(new LongWritable(1), new ByteWritable((byte) 777)));
    assertEquals(new ByteWritable((byte) 2),
      store.getTarget(new LongWritable(2), new ByteWritable((byte) 888)));
    assertNull(store.getTarget(new LongWritable(3), new ByteWritable((byte) 555)));
  }
}
