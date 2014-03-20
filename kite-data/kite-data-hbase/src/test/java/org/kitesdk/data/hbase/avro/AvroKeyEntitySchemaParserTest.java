/**
 * Copyright 2013 Cloudera Inc.
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
package org.kitesdk.data.hbase.avro;

import org.apache.avro.Schema.Type;
import org.junit.Test;
import org.kitesdk.data.ColumnMappingDescriptor;
import org.kitesdk.data.ColumnMappingDescriptor.MappingType;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.SchemaValidationException;

import static org.junit.Assert.assertEquals;

public class AvroKeyEntitySchemaParserTest {

  private static final AvroKeyEntitySchemaParser parser = new AvroKeyEntitySchemaParser();
  private static final String entitySchema = AvroUtils
      .inputStreamToString(AvroKeyEntitySchemaParserTest.class
          .getResourceAsStream("/TestRecord.avsc"));

  @Test
  public void testGoodSchema() {
    AvroEntitySchema avroEntitySchema = parser.parseEntitySchema(entitySchema);
    ColumnMappingDescriptor descriptor = avroEntitySchema
        .getColumnMappingDescriptor();
    assertEquals(9, descriptor.getFieldMappings().size());
    assertEquals(MappingType.COLUMN, descriptor.getFieldMapping("field1")
        .getMappingType());
    assertEquals(MappingType.KEY_AS_COLUMN, descriptor
        .getFieldMapping("field4").getMappingType());
    assertEquals(MappingType.OCC_VERSION, descriptor.getFieldMapping("version")
        .getMappingType());

    AvroKeySchema avroKeySchema = parser.parseKeySchema(entitySchema);
    assertEquals(Type.STRING, avroKeySchema.getAvroSchema()
        .getField("keyPart1").schema().getType());
    assertEquals(Type.STRING, avroKeySchema.getAvroSchema()
        .getField("keyPart2").schema().getType());
    assertEquals(2, avroKeySchema.getPartitionStrategy().getFieldPartitioners()
        .size());
  }

  @Test
  public void testOverridePartitionStrategy() {
    PartitionStrategy strat = new PartitionStrategy.Builder().hash("keyPart1",
        10).build();
    AvroKeySchema avroKeySchema = parser.parseKeySchema(entitySchema, strat);
    assertEquals(Type.INT, avroKeySchema.getAvroSchema().getField("keyPart1")
        .schema().getType());
    assertEquals(1, avroKeySchema.getPartitionStrategy().getFieldPartitioners()
        .size());
  }

  @Test
  public void testOverrideColumnMapping() {
    ColumnMappingDescriptor desc = new ColumnMappingDescriptor.Builder()
        .column("field1", "override:field1")
        .counter("version", "override:version").build();
    AvroEntitySchema avroEntitySchema = parser.parseEntitySchema(entitySchema,
        desc);
    desc = avroEntitySchema.getColumnMappingDescriptor();

    assertEquals(2, desc.getFieldMappings().size());
    assertEquals(MappingType.COLUMN, desc.getFieldMapping("field1")
        .getMappingType());
    assertEquals(MappingType.COUNTER, desc.getFieldMapping("version")
        .getMappingType());
  }

  @Test(expected = SchemaValidationException.class)
  public void testBadSchemaMultipleOCCVersions() {
    parser.parseEntitySchema(AvroUtils
        .inputStreamToString(AvroKeyEntitySchemaParser.class
            .getResourceAsStream("/BadSchemaMultipleOCCVersions.avsc")));
  }

  @Test(expected = SchemaValidationException.class)
  public void testBadSchemaOCCVersionAndCounter() {
    parser.parseEntitySchema(AvroUtils
        .inputStreamToString(AvroKeyEntitySchemaParser.class
            .getResourceAsStream("/BadSchemaOCCVersionAndCounter.avsc")));
  }
}
