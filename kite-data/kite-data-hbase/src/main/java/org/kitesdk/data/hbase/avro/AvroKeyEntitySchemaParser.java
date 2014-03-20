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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.kitesdk.data.ColumnMappingDescriptor;
import org.kitesdk.data.ColumnMappingDescriptor.MappingType;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.SchemaValidationException;
import org.kitesdk.data.hbase.impl.KeyEntitySchemaParser;
import org.kitesdk.data.spi.ColumnMappingDescriptorParser;
import org.kitesdk.data.spi.FieldMapping;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.PartitionStrategyParser;
import org.kitesdk.data.spi.partition.IdentityFieldPartitioner;

/**
 * This implementation parses the AvroKeySchema and AvroEntitySchema from Avro
 * schemas. The entities can contain metadata in annotations of the Avro record
 * and Avro record fields.
 * 
 * Each field must have a mapping annotation, which specifies how that field is
 * mapped to an HBase column.
 * 
 * Allowed mapping types are "key", "column", "keyAsColumn", and "occVersion".
 * 
 * The key mapping type on a field indicates that there is an identity field
 * partitioner on the field. The identity field partitioners are taken in order.
 * 
 * The column mapping type on a field tells this entity mapper to map that field
 * to the fully_qualified_column.
 * 
 * The keyAsColumn mapping type on a field tells the entity mapper to map each
 * key of the value type to a column in the specified column_family. This
 * annotation is only allowed on map and record types.
 * 
 * The occVersion mapping type on a field indicates that the entity participates
 * in optimistic concurrency control, and the field is the version number that
 * is automatically incremented by the system to validate that there are no
 * write conflicts.
 * 
 * Here is an example schema:
 * 
 * <pre>
 * 
 * { 
 *   "name": "record_name",
 *   "type": "record",
 *   "partitionStrategy": [
 *     { "sourceName": "field1", "type": "identity" }
 *   ],
 *   "fields": [ 
 *     { 
 *       "name": "field1", 
 *       "type": "int", 
 *       "mapping": { "type": "column", "value": "meta:field1" } 
 *     },
 *      
 *     { 
 *       "name": "field2", 
 *       "type": { "type": "map", "values": "string" }, 
 *       "mapping": { "type": "keyAsColumn": "value": "map_family" } 
 *     }
 *   ]
 * }
 * 
 * </pre>
 * 
 * An Avro instance of this schema would have its field1 value encoded to the
 * meta:field1 column. Each key/value pair of the field2 map type would have its
 * value mapped to the map_family:[key] column. It will also participate in
 * optimistic concurrency control.
 */
public class AvroKeyEntitySchemaParser implements
    KeyEntitySchemaParser<AvroKeySchema, AvroEntitySchema> {

  @Override
  public AvroKeySchema parseKeySchema(String rawSchema) {
    JsonNode schemaAsJson = rawSchemaAsJsonNode(rawSchema);
    Schema.Parser avroSchemaParser = new Schema.Parser();
    Schema schema = avroSchemaParser.parse(rawSchema);

    ColumnMappingDescriptorParser parser = new ColumnMappingDescriptorParser(
        schema);
    List<FieldMapping> fieldMappings = getFieldMappings(parser, schemaAsJson,
        schema);

    PartitionStrategy partitionStrategy;
    if (schemaAsJson.has("partitionStrategy")) {
      partitionStrategy = buildPartitionStrategyFromAnnotation(schema,
          schemaAsJson, fieldMappings);
    } else {
      partitionStrategy = buildPartitionStrategyFromKeyMappings(schema,
          fieldMappings);
    }
    return new AvroKeySchema(schema, rawSchema, partitionStrategy);
  }

  @Override
  public AvroKeySchema parseKeySchema(String rawSchema,
      PartitionStrategy partitionStrategy) {
    Schema.Parser avroSchemaParser = new Schema.Parser();
    Schema schema = avroSchemaParser.parse(rawSchema);
    return new AvroKeySchema(schema, rawSchema, partitionStrategy);
  }

  @Override
  public AvroEntitySchema parseEntitySchema(String rawSchema) {
    JsonNode schemaAsJson = rawSchemaAsJsonNode(rawSchema);
    Schema.Parser avroSchemaParser = new Schema.Parser();
    Schema schema = avroSchemaParser.parse(rawSchema);

    ColumnMappingDescriptorParser parser = new ColumnMappingDescriptorParser(
        schema);
    ColumnMappingDescriptor mappingDescriptor;
    if (schemaAsJson.has("mapping")) {
      mappingDescriptor = parser.parse(schemaAsJson.get("mapping").toString());
    } else {
      List<FieldMapping> fieldMappings = getFieldMappings(parser, schemaAsJson,
          schema);
      mappingDescriptor = new ColumnMappingDescriptor.Builder().fieldMappings(
          fieldMappings).build();
    }
    return new AvroEntitySchema(schema, rawSchema, mappingDescriptor);
  }

  @Override
  public AvroEntitySchema parseEntitySchema(String rawSchema,
      ColumnMappingDescriptor columnMappingDescriptor) {
    Schema.Parser avroSchemaParser = new Schema.Parser();
    Schema schema = avroSchemaParser.parse(rawSchema);
    return new AvroEntitySchema(schema, rawSchema, columnMappingDescriptor);
  }

  private JsonNode rawSchemaAsJsonNode(String rawSchema) {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode avroRecordSchemaJson;
    try {
      avroRecordSchemaJson = mapper.readValue(rawSchema, JsonNode.class);
    } catch (IOException e) {
      throw new SchemaValidationException(
          "Could not parse the avro record as JSON.", e);
    }
    return avroRecordSchemaJson;
  }

  private List<FieldMapping> getFieldMappings(
      ColumnMappingDescriptorParser parser, JsonNode schemaAsJson, Schema schema) {
    JsonNode fields = schemaAsJson.get("fields");
    if (fields == null) {
      throw new SchemaValidationException(
          "Avro Record Schema must contain fields");
    }

    // Build the fieldMappingMap, which is a mapping of field names to
    // AvroFieldMapping instances (which describe the mapping type of the
    // field).
    List<FieldMapping> fieldMappings = new ArrayList<FieldMapping>();
    for (JsonNode recordFieldJson : fields) {
      String fieldName = recordFieldJson.get("name").asText();
      Schema.Type type = schema.getField(fieldName).schema().getType();
      FieldMapping fieldMapping = createFieldMapping(parser, fieldName,
          recordFieldJson, type);
      if (fieldMapping != null) {
        fieldMappings.add(fieldMapping);
      }
    }

    return fieldMappings;
  }

  /**
   * Given a JsonNode representation of an avro record field, return the
   * AvroFieldMapping instance of that field. This instance contains the type of
   * mapping, and the value of that mapping, which will tell the mapping how to
   * map the field to columns in HBase.
   * 
   * @param parser
   *          The ColumnMappingDescriptorParser to use to parse FieldMappings.
   * @param fieldName
   *          The name of the field
   * @param recordFieldJson
   *          The Avro record field as a JsonNode.
   * @param type
   *          The field's java type
   * @return The FieldMapping of this field.
   */
  private FieldMapping createFieldMapping(ColumnMappingDescriptorParser parser,
      String fieldName, JsonNode recordFieldJson, Schema.Type type) {
    JsonNode mappingNode = recordFieldJson.get("mapping");
    if (mappingNode != null) {
      return parser.parseFieldMapping(fieldName, type, mappingNode);
    } else {
      return null;
    }
  }

  private PartitionStrategy buildPartitionStrategyFromAnnotation(Schema schema,
      JsonNode schemaAsJson, List<FieldMapping> fieldMappings) {
    PartitionStrategyParser partitionStrategyParser = new PartitionStrategyParser(
        schema);
    String partitionStrategyStr = schemaAsJson.get("partitionStrategy")
        .toString();
    PartitionStrategy partitionStrategy = partitionStrategyParser
        .parse(partitionStrategyStr);
    // now validate that any key mapping types are only sourced by id field
    // partitioners
    for (FieldMapping fieldMapping : fieldMappings) {
      if (fieldMapping.getMappingType() == MappingType.KEY) {
        for (FieldPartitioner fp : partitionStrategy.getFieldPartitioners()) {
          if (fp.getSourceName().equals(fieldMapping.getFieldName())
              && !fp.getClass().equals(IdentityFieldPartitioner.class)) {
            throw new SchemaValidationException(
                "KEY mapping only valid with IdentityFieldPartitioner. Not "
                    + fp.getClass().getName());
          }
        }
      }
    }
    return partitionStrategy;
  }

  private PartitionStrategy buildPartitionStrategyFromKeyMappings(
      Schema schema, List<FieldMapping> fieldMappings) {
    PartitionStrategy.Builder builder = new PartitionStrategy.Builder();
    for (FieldMapping fieldMapping : fieldMappings) {
      if (fieldMapping.getMappingType() == MappingType.KEY) {
        Class<?> fieldClass;
        Schema.Type schemaType = schema.getField(fieldMapping.getFieldName())
            .schema().getType();
        if (schemaType == Schema.Type.INT) {
          fieldClass = Integer.class;
        } else if (schemaType == Schema.Type.LONG) {
          fieldClass = Long.class;
        } else if (schemaType == Schema.Type.STRING) {
          fieldClass = String.class;
        } else {
          throw new SchemaValidationException(
              "KEY mapping type only valid for int, long, and string type. Given: "
                  + schemaType.toString());
        }
        builder.identity(fieldMapping.getFieldName(), fieldClass, 1);
      }
    }
    return builder.build();
  }
}
