package org.apache.iotdb.calcite;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.time.FastDateFormat;

public class IoTDBEnumerator<E> implements Enumerator<E> {

  private static final FastDateFormat TIME_FORMAT_DATE;
  private static final FastDateFormat TIME_FORMAT_TIME;
  private static final FastDateFormat TIME_FORMAT_TIMESTAMP;

  static {
    final TimeZone gmt = TimeZone.getTimeZone("GMT");
    TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt);
    TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt);
    TIME_FORMAT_TIMESTAMP =
        FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt);
  }

  private final AtomicBoolean cancelFlag;
  private final String[] filterValues;
  private final RowConverter<E> rowConverter;
  private Connection IoTDBConn;
  private final String storageGroupName;
  private ResultSet resultSet;
  private E current;

  IoTDBEnumerator(String storageGroup, AtomicBoolean cancelFlag, boolean stream,
      String[] filterValues, RowConverter<E> rowConverter) {
    this.cancelFlag = cancelFlag;
    this.rowConverter = rowConverter;
    this.filterValues = filterValues;
    this.storageGroupName = storageGroup;
    IoTDBConn = IoTDBSchema.getIoTDBConn();
    try {
      boolean hasResult = false;
      Statement statement = IoTDBConn.createStatement();
      String SqlTmp = "select * from %s";
      String sql = String.format(SqlTmp, storageGroupName);
      try {
        hasResult = statement.execute(sql);
      } catch (Exception e) {
        throw new RuntimeException("不存在时间序列");
      }
      if (hasResult) {
        resultSet = statement.getResultSet();
      }
    } catch (SQLException e) {
      throw new RuntimeException("不存在时间序列");
    }
  }

  /**
   * Returns an array of integers {0, ..., n - 1}.
   */
  static int[] identityList(int n) {
    int[] integers = new int[n];
    for (int i = 0; i < n; i++) {
      integers[i] = i;
    }
    return integers;
  }

  /**
   * Deduces the names and types of a table's columns by reading the first line of a CSV file.
   */
  static RelDataType deduceRowType(JavaTypeFactory typeFactory, String storageGroupName,
      List<IoTDBFieldType> fieldTypes, List<String> fieldNames) {
    final List<RelDataType> types = new ArrayList<>();
    final List<String> names = new ArrayList<>();
    Connection IoTDBconn = IoTDBSchema.getIoTDBConn();
    ResultSet resultSet = null;
    try {
      Statement statement = IoTDBconn.createStatement();
      String SqlTmp = "show timeseries %s";
      String Sql = String.format(SqlTmp, storageGroupName);
      boolean hasResult = statement.execute(Sql);
      if (hasResult) {
        resultSet = statement.getResultSet();
      }
      names.add("time");
      types.add(IoTDBFieldType.TIMESTAMP.toType(typeFactory));
      if (fieldTypes != null) {
        fieldTypes.add(IoTDBFieldType.TIMESTAMP);
      }
      if (fieldNames != null) {
        fieldNames.add("time");
      }
      while (resultSet.next()) {
        String name;
        final IoTDBFieldType ioTDBFieldType;
        name = resultSet.getString(1);
        String typeString = resultSet.getString(3);
        ioTDBFieldType = IoTDBFieldType.of(typeString);
        if (ioTDBFieldType == null) {
          System.out.println("WARNING: Found unknown type: "
              + typeString + " in : " + storageGroupName
              + " for column: " + name
              + ". Will assume the type of column is string");
        }
        final RelDataType type = ioTDBFieldType.toType(typeFactory);
        name = name.replace(storageGroupName.concat("."), "");
        names.add(name);
        types.add(type);
        if (fieldTypes != null) {
          fieldTypes.add(ioTDBFieldType);
        }
        if (fieldNames != null) {
          fieldNames.add(name);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return typeFactory.createStructType(Pair.zip(names, types));
  }

  /**
   * Row converter.
   *
   * @param <E> element type
   */
  abstract static class RowConverter<E> {

    abstract E convertRow(String[] rows);

    protected Object convert(IoTDBFieldType fieldType, String string) {
      if (fieldType == null) {
        return string;
      }
      switch (fieldType) {
        case BOOLEAN:
          if (string == null) {
            return null;
          }
          return Boolean.parseBoolean(string);
        case INT32:
          if (string == null) {
            return null;
          }
          return Integer.parseInt(string);
        case INT64:
          if (string == null) {
            return null;
          }
          return Long.parseLong(string);
        case FLOAT:
          if (string == null) {
            return null;
          }
          return Float.parseFloat(string);
        case DOUBLE:
          if (string == null) {
            return null;
          }
          return Double.parseDouble(string);
        case TIMESTAMP:
          if (string == null) {
            return null;
          }
          return Long.parseLong(string);
        case STRING:
        default:
          return string;
      }
    }
  }

  /**
   * Array row converter.
   */
  static class ArrayRowConverter extends RowConverter<Object[]> {

    private final IoTDBFieldType[] fieldTypes;
    private final int[] fields;
    // whether the row to convert is from a stream
    private final boolean stream;

    ArrayRowConverter(List<IoTDBFieldType> fieldTypes, int[] fields) {
      this.fieldTypes = fieldTypes.toArray(new IoTDBFieldType[0]);
      this.fields = fields;
      this.stream = false;
    }

    ArrayRowConverter(List<IoTDBFieldType> fieldTypes, int[] fields, boolean stream) {
      this.fieldTypes = fieldTypes.toArray(new IoTDBFieldType[0]);
      this.fields = fields;
      this.stream = stream;
    }

    public Object[] convertRow(String[] strings) {
      if (stream) {
        return convertStreamRow(strings);
      } else {
        return convertNormalRow(strings);
      }
    }

    public Object[] convertNormalRow(String[] strings) {
      final Object[] objects = new Object[fields.length];
      for (int i = 0; i < fields.length; i++) {
        int field = fields[i];
        objects[i] = convert(fieldTypes[field], strings[field]);
      }
      return objects;
    }

    public Object[] convertStreamRow(String[] strings) {
      final Object[] objects = new Object[fields.length + 1];
      objects[0] = System.currentTimeMillis();
      for (int i = 0; i < fields.length; i++) {
        int field = fields[i];
        objects[i + 1] = convert(fieldTypes[field], strings[field]);
      }
      return objects;
    }
  }

  @Override
  public E current() {
    return current;
  }

  @Override
  public boolean moveNext() {
    try {
      while (resultSet.next()) {
        ArrayList<String> list = new ArrayList<>();
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnNum = resultSetMetaData.getColumnCount();
        for (int i = 0; i < columnNum; i++) {
          list.add(resultSet.getString(i + 1));
        }
        String[] strings = new String[list.size()];
        list.toArray(strings);
        current = rowConverter.convertRow(strings);
        return true;
      }
      return false;
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
  }
}
