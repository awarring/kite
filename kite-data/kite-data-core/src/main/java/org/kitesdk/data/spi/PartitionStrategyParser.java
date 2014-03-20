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
import org.apache.avro.Schema.Type;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.SchemaValidationException;

/**
 * Parses a PartitionStrategy from JSON representation.
 * 
 * <pre>
 * [
 *   { "type": "identity", "sourceName": "id" },
 *   { "type": "year", "sourceName": "created" },
 *   { "type": "month", "sourceName": "created" },
 *   { "type": "day", "sourceName": "created" }
 * ]
 * </pre>
 * 
 */
public class PartitionStrategyParser {

  private final Schema schema;

  public PartitionStrategyParser(Schema schema) {
    this.schema = schema;
  }

  /**
   * Parses a PartitionStrategy from a JSON string.
   * 
   * @param json
   *          The JSON string
   * @return The PartitionStrategy.
   */
  public PartitionStrategy parse(String json) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode node = mapper.readValue(json, JsonNode.class);
      return buildPartitionStrategy(node);
    } catch (JsonParseException e) {
      throw new SchemaValidationException("Invalid JSON", e);
    } catch (JsonMappingException e) {
      throw new SchemaValidationException("Invalid JSON", e);
    } catch (IOException e) {
      throw new SchemaValidationException(e);
    }
  }

  /**
   * Parses a PartitionStrategy from a File
   * 
   * @param file
   *          The File that contains the PartitionStrategy in JSON format.
   * @return The PartitionStrategy.
   */
  public PartitionStrategy parse(File file) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode node = mapper.readValue(file, JsonNode.class);
      return buildPartitionStrategy(node);
    } catch (JsonParseException e) {
      throw new SchemaValidationException("Invalid JSON", e);
    } catch (JsonMappingException e) {
      throw new SchemaValidationException("Invalid JSON", e);
    } catch (IOException e) {
      throw new SchemaValidationException(e);
    }
  }

  /**
   * Parses a PartitionStrategy from an input stream
   * 
   * @param in
   *          The input stream that contains the PartitionStrategy in JSON
   *          format.
   * @return The PartitionStrategy.
   */
  public PartitionStrategy parse(InputStream in) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode node = mapper.readValue(in, JsonNode.class);
      return buildPartitionStrategy(node);
    } catch (JsonParseException e) {
      throw new SchemaValidationException("Invalid JSON", e);
    } catch (JsonMappingException e) {
      throw new SchemaValidationException("Invalid JSON", e);
    } catch (IOException e) {
      throw new SchemaValidationException(e);
    }
  }

  private PartitionStrategy buildPartitionStrategy(JsonNode node) {
    PartitionStrategy.Builder builder = new PartitionStrategy.Builder();
    for (Iterator<JsonNode> it = node.elements(); it.hasNext();) {
      JsonNode fieldPartitioner = it.next();
      if (!fieldPartitioner.has("type")) {
        throw new SchemaValidationException(
            "FieldPartitioner must contain type attribute.");
      }
      if (!fieldPartitioner.has("sourceName")) {
        throw new SchemaValidationException(
            "FieldPartitioner must contain sourceName attribute.");
      }
      String type = fieldPartitioner.get("type").asText();
      String sourceName = fieldPartitioner.get("sourceName").asText();
      if (schema.getField(sourceName) == null) {
        throw new SchemaValidationException(
            "Invalid sourceName on FieldPartitioner: " + type);
      }

      if (type.equals("identity")) {
        Class<?> sourceClass = getSchemaType(schema.getField(sourceName)
            .schema());
        builder.identity(sourceName, sourceName, sourceClass, 0);
      } else if (type.equals("year")) {
        builder.year(sourceName);
      } else if (type.equals("month")) {
        builder.month(sourceName);
      } else if (type.equals("day")) {
        builder.day(sourceName);
      } else if (type.equals("dateFormat")) {
        String format = fieldPartitioner.get("format").asText();
        builder.dateFormat(sourceName, sourceName, format);
      } else if (type.equals("hour")) {
        builder.hour(sourceName);
      } else if (type.equals("minute")) {
        builder.minute(sourceName);
      } else if (type.equals("hash")) {
        int buckets = Integer
            .parseInt(fieldPartitioner.get("buckets").asText());
        builder.hash(sourceName, buckets);
      } else {
        throw new SchemaValidationException("Invalid FieldPartitioner: " + type);
      }
    }
    return builder.build();
  }

  private Class<?> getSchemaType(Schema s) {
    Type schemaType = s.getType();
    if (schemaType == Type.INT) {
      return Integer.class;
    } else if (schemaType == Type.LONG) {
      return Long.class;
    } else if (schemaType == Type.FLOAT) {
      return Float.class;
    } else if (schemaType == Type.DOUBLE) {
      return Double.class;
    } else if (schemaType == Type.BOOLEAN) {
      return Boolean.class;
    } else if (schemaType == Type.BYTES) {
      return byte[].class;
    } else if (schemaType == Type.STRING) {
      return String.class;
    } else {
      throw new SchemaValidationException("Invalid FieldPartitioner Type: "
          + schemaType.toString());
    }
  }

}
