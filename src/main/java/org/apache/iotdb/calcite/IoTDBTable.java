package org.apache.iotdb.calcite;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

public class IoTDBTable extends AbstractTable {

  public final String storageGroupName;
  public List<String> fieldNames;
  public List<IoTDBFieldType> fieldTypes;

  public IoTDBTable(String storageGroup) {
    this.storageGroupName = storageGroup;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (fieldTypes == null) {
      fieldNames = new ArrayList<>();
      fieldTypes = new ArrayList<>();
      return IoTDBEnumerator.deduceRowType((JavaTypeFactory) typeFactory, storageGroupName,
          fieldTypes, fieldNames);
    } else {
      return IoTDBEnumerator
          .deduceRowType((JavaTypeFactory) typeFactory, storageGroupName, null, null);
    }
  }

  /**
   * Various degrees of table "intelligence".
   */
  public enum Flavor {
    SCANNABLE, PROFIL
  }
}
