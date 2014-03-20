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
import org.kitesdk.data.ColumnMappingDescriptor;
import org.kitesdk.data.SchemaValidationException;
import org.kitesdk.data.ColumnMappingDescriptor.MappingType;

import static org.junit.Assert.assertEquals;

public class TestColumnMappingDescriptorParser {
  
  private Schema schema;
  private ColumnMappingDescriptorParser parser;
  
  @Before
  public void before() throws IOException {
    InputStream userIn = TestPartitionStrategyParser.class
        .getResourceAsStream("/schema/user.avsc");
    Schema.Parser schemaParser = new Schema.Parser();
    schema = schemaParser.parse(userIn);
    parser = new ColumnMappingDescriptorParser(schema);
  }
  
  @Test
  public void testBasic() {
    InputStream in = TestColumnMappingDescriptorParser.class
        .getResourceAsStream("/column_mapping/UserColumnMapping.json");
    ColumnMappingDescriptor desc = parser.parse(in);
    
    FieldMapping usernameFieldMapping = desc.getFieldMapping("username");
    FieldMapping emailFieldMapping = desc.getFieldMapping("email");
    
    assertEquals(2, desc.getFieldMappings().size());
    assertEquals("meta:username", usernameFieldMapping.getMappingValue());
    assertEquals("meta:email", emailFieldMapping.getMappingValue());
    assertEquals(MappingType.COLUMN, usernameFieldMapping.getMappingType());
    assertEquals(MappingType.COLUMN, emailFieldMapping.getMappingType());
  }
  
  @Test(expected = SchemaValidationException.class)
  public void testInvalidSource() {
    InputStream in = TestColumnMappingDescriptorParser.class
        .getResourceAsStream("/column_mapping/InvalidSourceColumnMapping.json");
    parser.parse(in);
  }
}
