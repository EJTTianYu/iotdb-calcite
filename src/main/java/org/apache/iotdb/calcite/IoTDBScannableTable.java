package org.apache.iotdb.calcite;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.schema.ScannableTable;

public class IoTDBScannableTable extends IoTDBTable implements ScannableTable {

  public IoTDBScannableTable(String storageGroup) {
    super(storageGroup);
  }

  public String toString() {
    return "IoTDBScannableTable";
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    final int[] fields = IoTDBEnumerator.identityList(fieldTypes.size());
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new IoTDBEnumerator<>(storageGroupName, cancelFlag, false, null,
            new IoTDBEnumerator.ArrayRowConverter(fieldTypes, fields));
      }
    };
  }
}
