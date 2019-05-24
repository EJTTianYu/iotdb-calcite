package org.apache.iotdb.calcite;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;

public class IoTDBProjectFilterTable extends IoTDBTable implements ProjectableFilterableTable {

//  private boolean firstProject = true;
//  private String filterString;
//  private String projectString;
//  private List<IoTDBFieldType> fieldTypesAft;
//  private int[] fields;

  public IoTDBProjectFilterTable(String storageGroup) {
    super(storageGroup);
  }

  public String toString() {
    return "IoTDBProjectFilterTable";
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
    String filterString = processFilters(filters);
    String projectString = processProject(projects);
//    System.out.println(filterString);
//    System.out.println(projectString);
    int[] fields = projects != null ? IoTDBPFEnumerator.identityList(projects)
        : IoTDBEnumerator.identityList(fieldTypes.size());
    List<IoTDBFieldType> fieldTypesAft =
        projects != null ? processFieldTypes(fieldTypes, projects) : fieldTypes;

    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new IoTDBPFEnumerator<>(storageGroupName, projectString, filterString, cancelFlag,
            false, null,
            new IoTDBPFEnumerator.ArrayRowConverter(fieldTypesAft, fields));
      }
    };
  }

  private String processFilters(List<RexNode> filters) {
    if (filters.size() != 0) {
      StringBuilder filterString = new StringBuilder("where ");
      for (RexNode filterNode : filters) {
        RexCall filterCall = (RexCall) filterNode;
//      String filterPre = filterNode.toString();
//      StringBuilder filterSubString = new StringBuilder();
//      filterSubString
//          .append(this.fieldNames.get(
//              ((RexInputRef) ((RexCall) filterCall.operands.get(0)).operands.get(0)).getIndex()));
//      filterSubString.append(filterCall.op.toString());
//      filterSubString.append(filterCall.operands.get(1).toString());
//      filterString.append(filterSubString).append(" and ");
        filterString = filterString.append(processTree(filterCall));
      }
//    System.out.println(filterString);
      return filterString.toString();
    } else {
      return "";
    }
  }

  private String processTree(RexCall filter) {
    StringBuilder filterString = new StringBuilder();
    //如果发现叶节点
    if (filter.getOperands().get(0) instanceof RexInputRef) {
      filterString
          .append((this.fieldNames.get(((RexInputRef) filter.getOperands().get(0)).getIndex())));
      filterString.append(filter.getOperator().toString());
      filterString.append(filter.getOperands().get(1));
      return filterString.toString();
    } else {
      filterString.append("(").append(processTree((RexCall) filter.getOperands().get(0)))
          .append(") ");
      filterString.append(filter.getOperator().toString());
      filterString.append(" (").append(processTree((RexCall) filter.getOperands().get(1)))
          .append(")");
    }
//    System.out.println(filterString);
    return filterString.toString();
  }

  private String processProject(int[] projects) {
    if (projects != null) {
      StringBuilder projectString = new StringBuilder();
      for (int i : projects) {
        if (i != 0) {
          projectString.append(this.fieldNames.get(i) + ',');
        }
      }
      if (projectString.length() == 0) {
        return "*";
      } else {
        return projectString.toString().substring(0, projectString.length() - 1);
      }
    } else {
      return "*";
    }
  }

  private List<IoTDBFieldType> processFieldTypes(List<IoTDBFieldType> fieldTypeList,
      int[] projects) {
    List<IoTDBFieldType> fieldTypesAft = new ArrayList<>();
//    fieldTypesAft.add(fieldTypeList.get(0));
    for (int i : projects) {
//      if (i != 0) {
      fieldTypesAft.add(fieldTypeList.get(i));
//      }
    }
    return fieldTypesAft;
  }
}
