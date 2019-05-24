package org.apache.iotdb.calcite;

import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Type of a field in a CSV file.
 *
 * <p>Usually, and unless specified explicitly in the header row, a field is
 * of type {@link #STRING}. But specifying the field type in the header row makes it easier to write
 * SQL.</p>
 */
enum IoTDBFieldType {
  STRING(String.class, "TEXT"),
  BOOLEAN(Boolean.class, "BOOLEAN"),
  INT32(Integer.class, "INT32"),
  INT64(Long.class, "INT64"),
  FLOAT(Float.class, "FLOAT"),
  DOUBLE(Double.class, "DOUBLE"),
  TIMESTAMP(Long.class, "timestamp");

  private final Class clazz;
  private final String simpleName;

  private static final Map<String, IoTDBFieldType> MAP = new HashMap<>();

  static {
    for (IoTDBFieldType value : values()) {
      MAP.put(value.simpleName, value);
    }
  }

  IoTDBFieldType(Class clazz, String simpleName) {
    this.clazz = clazz;
    this.simpleName = simpleName;
  }

  public RelDataType toType(JavaTypeFactory typeFactory) {
    RelDataType javaType = typeFactory.createJavaType(clazz);
    RelDataType sqlType = typeFactory.createSqlType(javaType.getSqlTypeName());
    return typeFactory.createTypeWithNullability(sqlType, true);
  }

  public static IoTDBFieldType of(String typeString) {
    return MAP.get(typeString);
  }

  public static void main(String[] args) {
    for (IoTDBFieldType IoTDBFieldType : values()) {
      System.out.print(IoTDBFieldType.simpleName + " ");
    }
  }
}

// End CsvFieldType.java
