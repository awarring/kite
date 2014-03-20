/**
 * Copyright 2014 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.spi;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.SchemaValidationException;
import org.kitesdk.data.spi.PartitionStrategyParser;

import static org.junit.Assert.assertEquals;

public class TestPartitionStrategyParser {

  private Schema schema;
  private PartitionStrategyParser partStratParser;

  @Before
  public void before() throws IOException {
    InputStream userIn = TestPartitionStrategyParser.class
        .getResourceAsStream("/schema/user.avsc");
    Schema.Parser schemaParser = new Schema.Parser();
    schema = schemaParser.parse(userIn);
    partStratParser = new PartitionStrategyParser(schema);
  }

  @Test
  public void testValid() {
    InputStream partStratIn = TestPartitionStrategyParser.class
        .getResourceAsStream("/partition_strategy/UserPartitionStrategy.json");
    PartitionStrategy partStrat = partStratParser.parse(partStratIn);

    assertEquals(1, partStrat.getFieldPartitioners().size());
    assertEquals("email", partStrat.getFieldPartitioners().get(0)
        .getSourceName());
  }

  @Test(expected = SchemaValidationException.class)
  public void testInvalidSource() {
    InputStream partStratIn = TestPartitionStrategyParser.class
        .getResourceAsStream("/partition_strategy/InvalidSourceUserPartitionStrategy.json");
    partStratParser.parse(partStratIn);
  }
  
  @Test(expected = SchemaValidationException.class)
  public void testInvalidFieldPartitioner() {
    InputStream partStratIn = TestPartitionStrategyParser.class
        .getResourceAsStream("/partition_strategy/InvalidFieldPartitionerPartitionStrategy.json");
    partStratParser.parse(partStratIn);
  }
  
  @Test(expected = SchemaValidationException.class)
  public void testNoSourceName() {
    InputStream partStratIn = TestPartitionStrategyParser.class
        .getResourceAsStream("/partition_strategy/NoSourceNamePartitionStrategy.json");
    partStratParser.parse(partStratIn);
  }
  
  @Test(expected = SchemaValidationException.class)
  public void testNoType() {
    InputStream partStratIn = TestPartitionStrategyParser.class
        .getResourceAsStream("/partition_strategy/NoTypePartitionStrategy.json");
    partStratParser.parse(partStratIn);
  }
}
