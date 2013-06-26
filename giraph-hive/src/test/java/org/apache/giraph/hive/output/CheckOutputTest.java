package org.apache.giraph.hive.output;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.hive.GiraphHiveTestBase;
import org.apache.giraph.hive.common.GiraphHiveConstants;
import org.apache.giraph.hive.computations.ComputationCountEdges;
import org.apache.giraph.hive.output.examples.HiveOutputIntIntVertex;
import org.apache.giraph.io.formats.IntNullTextEdgeInputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.facebook.hiveio.common.HiveMetastores;
import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.output.HiveOutputDescription;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.facebook.hiveio.schema.TestSchema;
import com.facebook.hiveio.testing.LocalHiveServer;

import java.io.IOException;

import static com.facebook.hiveio.record.HiveRecordFactory.newWritableRecord;
import static org.junit.Assert.assertNull;

public class CheckOutputTest extends GiraphHiveTestBase {
  private LocalHiveServer hiveServer = new LocalHiveServer("giraph-hive");

  @Before
  public void setUp() throws IOException, TException {
    hiveServer.init();
    HiveMetastores.setTestClient(hiveServer.getClient());
  }

  @Test
  public void testCheck() throws Exception {
    VertexToHive vertexToHive = new HiveOutputIntIntVertex();
    HiveOutputDescription outputDesc = new HiveOutputDescription();
    HiveTableSchema schema = TestSchema.builder()
        .addColumn("foo", HiveType.LONG)
        .addColumn("bar", HiveType.LONG)
        .build();
    vertexToHive.checkOutput(outputDesc, schema, newWritableRecord(schema));

    schema = TestSchema.builder()
            .addColumn("foo", HiveType.INT)
            .addColumn("bar", HiveType.LONG)
            .build();
    checkThrows(vertexToHive, outputDesc, schema);
  }

  private void checkThrows(VertexToHive vertexToHive,
      HiveOutputDescription outputDesc, HiveTableSchema schema) {
    try {
      vertexToHive.checkOutput(outputDesc, schema, newWritableRecord(schema));
    } catch (IllegalArgumentException e) {
      return;
    }
    Assert.fail();
  }

  @Test
  public void testCheckFailsJob() throws Exception {
    String tableName = "test1";
    hiveServer.createTable("CREATE TABLE " + tableName +
       " (i1 INT, i2 BIGINT) ");

    GiraphConfiguration conf = new GiraphConfiguration();
    String[] edges = new String[] {
        "1 2",
        "2 3",
        "2 4",
        "4 1"
    };

    GiraphHiveConstants.HIVE_VERTEX_OUTPUT_TABLE.set(conf, tableName);
    GiraphHiveConstants.VERTEX_TO_HIVE_CLASS.set(conf, HiveOutputIntIntVertex.class);

    conf.setComputationClass(ComputationCountEdges.class);
    conf.setOutEdgesClass(ByteArrayEdges.class);
    conf.setEdgeInputFormatClass(IntNullTextEdgeInputFormat.class);
    conf.setVertexOutputFormatClass(HiveVertexOutputFormat.class);
    try {
      Iterable<String> result = InternalVertexRunner.run(conf, null, edges);
      assertNull(result);
    } catch (IllegalArgumentException e) { }
  }
}
