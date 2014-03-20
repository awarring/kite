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
package org.kitesdk.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.kitesdk.data.spi.FieldMapping;

/**
 * A descriptor for an entity's column mappings, which defines how an entity
 * maps to a columnar store.
 */
public class ColumnMappingDescriptor {
  
  private final Collection<FieldMapping> fieldMappings;
  
  private ColumnMappingDescriptor(Builder builder) {
    validateFieldMappings(builder.fieldMappings);
    fieldMappings = builder.fieldMappings;
  }

  public Collection<FieldMapping> getFieldMappings() {
    return fieldMappings;
  }

  public FieldMapping getFieldMapping(String fieldName) {
    for (FieldMapping fm : fieldMappings) {
      if (fm.getFieldName().equals(fieldName)) {
        return fm;
      }
    }
    return null;
  }

  /**
   * Get the columns required by this schema.
   * 
   * @return The set of columns
   */
  public Set<String> getRequiredColumns() {
    Set<String> set = new HashSet<String>();
    for (FieldMapping fieldMapping : fieldMappings) {
      if (MappingType.KEY == fieldMapping.getMappingType()) {
        continue;
      } else if (MappingType.KEY_AS_COLUMN == fieldMapping.getMappingType()) {
        String family = new String(fieldMapping.getFamily()) + ":";
        set.add(family);
      } else {
        set.add(new String(fieldMapping.getFamily()) + ":"
            + new String(fieldMapping.getQualifier()));
      }
    }
    return set;
  }

  /**
   * Get the column families required by this schema.
   * 
   * @return The set of column families.
   */
  public Set<String> getRequiredColumnFamilies() {
    Set<String> set = new HashSet<String>();
    Set<String> columnSet = getRequiredColumns();
    for (String column : columnSet) {
      set.add(column.split(":")[0]);
    }
    return set;
  }

  /**
   * Validate that the field mappings provided for this schema are compatible
   * with a valid schema. The rules are:
   * 
   * <pre>
   * 1. An entity schema can't contain multiple occVersion mapping fields
   * 2. An entity schema can't contain both an occVersion field and a counter 
   *    field.
   * </pre>
   * 
   * This method will throw a SchemaValidationException if any of these rules
   * are violated. Otherwise, no exception is thrown.
   * 
   * @param fieldMappings
   *          The collection of FieldMappings to validate
   */
  private void validateFieldMappings(Collection<FieldMapping> fieldMappings) {
    boolean hasOCCVersion = false;
    boolean hasCounter = false;

    for (FieldMapping fieldMapping : fieldMappings) {
      if (fieldMapping.getMappingType() == MappingType.OCC_VERSION) {
        if (hasOCCVersion) {
          throw new SchemaValidationException(
              "Schema can't contain multiple occVersion fields.");
        }
        if (hasCounter) {
          throw new SchemaValidationException(
              "Schema can't contain both an occVersion field and a counter field.");
        }
        hasOCCVersion = true;
      } else if (fieldMapping.getMappingType() == MappingType.COUNTER) {
        if (hasOCCVersion) {
          throw new SchemaValidationException(
              "Schema can't contain both an occVersion field and a counter field.");
        }
        hasCounter = true;
      }
    }
  }

  /**
   * A fluent builder to aid in the construction of {@link MappingDescriptor}s.
   */
  public static class Builder {

    private Collection<FieldMapping> fieldMappings = new ArrayList<FieldMapping>();

    public Builder column(String name, String column) {
      fieldMappings
          .add(new FieldMapping(name, MappingType.COLUMN, column, null));
      return this;
    }

    public Builder keyAsColumn(String name, String columnFamily) {
      fieldMappings.add(new FieldMapping(name, MappingType.KEY_AS_COLUMN,
          columnFamily, null));
      return this;
    }

    public Builder keyAsColumn(String name, String columnFamily, String prefix) {
      fieldMappings.add(new FieldMapping(name, MappingType.KEY_AS_COLUMN,
          columnFamily, prefix));
      return this;
    }

    public Builder counter(String name, String column) {
      fieldMappings
          .add(new FieldMapping(name, MappingType.COUNTER, column, null));
      return this;
    }

    public Builder occ(String name) {
      fieldMappings.add(new FieldMapping(name, MappingType.OCC_VERSION, null,
          null));
      return this;
    }

    public Builder fieldMapping(FieldMapping fieldMapping) {
      fieldMappings.add(fieldMapping);
      return this;
    }

    public Builder fieldMappings(Collection<FieldMapping> fieldMappings) {
      this.fieldMappings.addAll(fieldMappings);
      return this;
    }

    public ColumnMappingDescriptor build() {
      return new ColumnMappingDescriptor(this);
    }
  }
  
  /**
   * The supported Mapping Types, which control how an entity field maps to
   * columns in an HBase table.
   */
  public static enum MappingType {

    // Maps a value to a part of the row key
    KEY,
    
    // Maps a value to a single column.
    COLUMN,

    // Maps a map or record value to columns
    // in a column family.
    KEY_AS_COLUMN,
    
    // Maps a field to one that can be incremented
    COUNTER,
    
    // The field will be populated with the
    // current version of the entity. This
    // allows the version to be checked if this
    // same entity is persisted back, to make sure
    // it hasn't changed.
    OCC_VERSION
  }
}
