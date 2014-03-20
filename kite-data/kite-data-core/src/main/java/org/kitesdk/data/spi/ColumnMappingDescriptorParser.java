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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.kitesdk.data.ColumnMappingDescriptor;
import org.kitesdk.data.ColumnMappingDescriptor.MappingType;
import org.kitesdk.data.SchemaValidationException;

/**
 * Parser for ColumnMappingDescriptor. Will parse the mapping annotation from
 * Avro schemas, and will parse the ColumnMappingDescriptor JSON format. An
 * example of that is:
 * 
 * <pre>
 * [
 *   { "source": "field1", "type": "column", "value": "cf:field1" },
 *   { "source": "field2", "type": "keyAsColumn", "value": "kac:" },
 *   { "source": "field3", "type": "occVersion" }
 * ]
 * </pre>
 * 
 */
public class ColumnMappingDescriptorParser {

  private final Schema schema;

  public ColumnMappingDescriptorParser(Schema schema) {
    this.schema = schema;
  }

  /**
   * Parses the Mapping Descriptor as a JSON string.
   * 
   * @param mappingDescriptor
   *          The mapping descriptor as a JSON string
   * @return ColumnMappingDescriptor
   */
  public ColumnMappingDescriptor parse(String mappingDescriptor) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode node = mapper.readValue(mappingDescriptor, JsonNode.class);
      return buildColumnMappingDescriptor(node);
    } catch (JsonParseException e) {
      throw new SchemaValidationException("Invalid JSON", e);
    } catch (JsonMappingException e) {
      throw new SchemaValidationException("Invalid JSON", e);
    } catch (IOException e) {
      throw new SchemaValidationException(e);
    }
  }

  /**
   * Parses the Mapping Descriptor from a File
   * 
   * @param file
   *          The File that contains the Mapping Descriptor in JSON format.
   * @return ColumnMappingDescriptor.
   */
  public ColumnMappingDescriptor parse(File file) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode node = mapper.readValue(file, JsonNode.class);
      return buildColumnMappingDescriptor(node);
    } catch (JsonParseException e) {
      throw new SchemaValidationException("Invalid JSON", e);
    } catch (JsonMappingException e) {
      throw new SchemaValidationException("Invalid JSON", e);
    } catch (IOException e) {
      throw new SchemaValidationException(e);
    }
  }

  /**
   * Parses the Mapping Descriptor from an input stream
   * 
   * @param in
   *          The input stream that contains the Mapping Descriptor in JSON
   *          format.
   * @return ColumnMappingDescriptor.
   */
  public ColumnMappingDescriptor parse(InputStream in) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode node = mapper.readValue(in, JsonNode.class);
      return buildColumnMappingDescriptor(node);
    } catch (JsonParseException e) {
      throw new SchemaValidationException("Invalid JSON", e);
    } catch (JsonMappingException e) {
      throw new SchemaValidationException("Invalid JSON", e);
    } catch (IOException e) {
      throw new SchemaValidationException(e);
    }
  }

  /**
   * Prases the FieldMapping from an annotated schema field.
   * 
   * @param fieldName
   *          The field name
   * @param type
   *          The type of the field
   * @param mappingNode
   *          The value of the "mapping" node
   * @return FieldMapping
   */
  public FieldMapping parseFieldMapping(String fieldName, Schema.Type type,
      JsonNode mappingNode) {
    JsonNode mappingTypeNode = mappingNode.get("type");
    JsonNode mappingValueNode = mappingNode.get("value");
    JsonNode prefixValueNode = mappingNode.get("prefix");
    if (mappingTypeNode == null) {
      String msg = "mapping attribute must contain type.";
      throw new SchemaValidationException(msg);
    }

    MappingType mappingType = null;
    String mappingValue = null;
    String prefix = null;

    if (mappingTypeNode.asText().equals("column")) {
      if (mappingValueNode == null) {
        throw new SchemaValidationException(
            "column mapping type must contain a value.");
      }
      mappingType = MappingType.COLUMN;
      mappingValue = mappingValueNode.asText();
    } else if ((mappingTypeNode.asText().equals("keyAsColumn"))) {
      if (type != Schema.Type.MAP && type != Schema.Type.RECORD) {
        throw new SchemaValidationException(
            "keyAsColumn mapping type only valid for MAP and RECORD field types. Type was: "
                + type.toString());
      }
      if (mappingValueNode == null) {
        throw new SchemaValidationException(
            "keyAsColumn mapping type must contain a value.");
      }
      mappingType = MappingType.KEY_AS_COLUMN;
      mappingValue = mappingValueNode.asText();
      if (prefixValueNode != null) {
        prefix = prefixValueNode.asText();
      }
    } else if (mappingTypeNode.asText().equals("counter")) {
      if (type != Schema.Type.INT && type != Schema.Type.LONG) {
        throw new SchemaValidationException(
            "counter mapping type must be an int or a long");
      }
      if (mappingValueNode == null) {
        throw new SchemaValidationException(
            "counter mapping type must contain a value.");
      }
      mappingType = MappingType.COUNTER;
      mappingValue = mappingValueNode.asText();
    } else if (mappingTypeNode.asText().equals("occVersion")) {
      if (type != Schema.Type.INT && type != Schema.Type.LONG) {
        throw new SchemaValidationException(
            "occVersion mapping type must be an int or a long");
      }
      mappingType = MappingType.OCC_VERSION;
    } else if (mappingTypeNode.asText().equals("key")) {
      mappingType = MappingType.KEY;
    }
    return new FieldMapping(fieldName, mappingType, mappingValue, prefix);
  }

  private ColumnMappingDescriptor buildColumnMappingDescriptor(JsonNode node) {
    ColumnMappingDescriptor.Builder builder = new ColumnMappingDescriptor.Builder();
    for (Iterator<JsonNode> it = node.elements(); it.hasNext();) {
      JsonNode mappingNode = it.next();
      if (!mappingNode.has("source")) {
        throw new SchemaValidationException("Mapping node needs a source: "
            + mappingNode.toString());
      }
      String source = mappingNode.get("source").asText();
      Field field = schema.getField(source);
      if (field == null) {
        throw new SchemaValidationException("Source '" + source
            + "' references unknown schema field");
      }
      builder.fieldMapping(parseFieldMapping(source, field.schema().getType(),
          mappingNode));
    }
    return builder.build();
  }
}
