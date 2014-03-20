package org.kitesdk.data.spi;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.kitesdk.data.ColumnMappingDescriptor.MappingType;
import org.kitesdk.data.SchemaValidationException;

public class FieldMapping {
  
  private static byte[] SYS_COL_FAMILY = "_s".getBytes();
  private static byte[] VERSION_CHECK_COL_QUALIFIER = "w".getBytes();

  private final String fieldName;
  private final MappingType mappingType;
  private final String mappingValue;
  private final String prefix;
  private final byte[] family;
  private final byte[] qualifier;

  public FieldMapping(String fieldName, MappingType mappingType,
      String mappingValue, String prefix) {
    this.fieldName = fieldName;
    this.mappingType = mappingType;
    this.mappingValue = mappingValue;
    this.prefix = prefix;
    this.family = getFamilyFromMappingValue(mappingValue);
    this.qualifier = getQualifierFromMappingValue(mappingValue);
  }

  public String getFieldName() {
    return fieldName;
  }

  public MappingType getMappingType() {
    return mappingType;
  }

  public String getMappingValue() {
    return mappingValue;
  }

  public String getPrefix() {
    return prefix;
  }

  public byte[] getFamily() {
    return family;
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  private byte[] getFamilyFromMappingValue(String mappingValue) {
    if (mappingType == MappingType.KEY) {
      return null;
    } else if (mappingType == MappingType.OCC_VERSION) {
      return SYS_COL_FAMILY;
    } else {
      String[] familyQualifier = mappingValue.split(":", 2);
      byte[] family;
      try {
        family = familyQualifier[0].getBytes("UTF-8");
      } catch (UnsupportedEncodingException exc) {
        throw new SchemaValidationException(
            "fieldType Must support UTF-8 encoding", exc);
      }
      return family;
    }
  }

  private byte[] getQualifierFromMappingValue(String mappingValue) {
    if (mappingType == MappingType.KEY) {
      return null;
    } else if (mappingType == MappingType.OCC_VERSION) {
      return VERSION_CHECK_COL_QUALIFIER;
    } else {
      String[] familyQualifier = mappingValue.split(":", 2);
      byte[] qualifier;
      try {
        qualifier = familyQualifier.length == 1 ? new byte[0]
            : familyQualifier[1].getBytes("UTF-8");
      } catch (UnsupportedEncodingException exc) {
        throw new SchemaValidationException(
            "fieldType Must support UTF-8 encoding", exc);
      }
      return qualifier;
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(family);
    result = prime * result
        + ((fieldName == null) ? 0 : fieldName.hashCode());
    result = prime * result
        + ((mappingType == null) ? 0 : mappingType.hashCode());
    result = prime * result
        + ((mappingValue == null) ? 0 : mappingValue.hashCode());
    result = prime * result + ((prefix == null) ? 0 : prefix.hashCode());
    result = prime * result + Arrays.hashCode(qualifier);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    FieldMapping other = (FieldMapping) obj;
    if (!Arrays.equals(family, other.family))
      return false;
    if (fieldName == null) {
      if (other.fieldName != null)
        return false;
    } else if (!fieldName.equals(other.fieldName))
      return false;
    if (mappingType != other.mappingType)
      return false;
    if (mappingValue == null) {
      if (other.mappingValue != null)
        return false;
    } else if (!mappingValue.equals(other.mappingValue))
      return false;
    if (prefix == null) {
      if (other.prefix != null)
        return false;
    } else if (!prefix.equals(other.prefix))
      return false;
    if (!Arrays.equals(qualifier, other.qualifier))
      return false;
    return true;
  }
}