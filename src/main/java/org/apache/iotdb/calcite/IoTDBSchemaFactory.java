package org.apache.iotdb.calcite;

import java.sql.SQLException;
import java.util.Map;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.iotdb.calcite.IoTDBTable.Flavor;

public class IoTDBSchemaFactory implements SchemaFactory {

  /**
   * Public singleton, per factory contract.
   */
  public static final IoTDBSchemaFactory INSTANCE = new IoTDBSchemaFactory();

  public IoTDBSchemaFactory() {

  }

  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    final String host = (String) operand.get("host");
    final String userName = (String) operand.get("userName");
    final String password = (String) operand.get("password");
    final String flavorString = (String) operand.get("flavor");
    IoTDBTable.Flavor flavor = null;
    if (flavorString.equals("scan")) {
      flavor = Flavor.SCANNABLE;
    } else {
      flavor = Flavor.PROFIL;
    }

    int port = 6667;
    if (operand.containsKey("port")) {
      Object portObj = operand.get("port");
      if (portObj instanceof String) {
        port = Integer.parseInt((String) portObj);
      } else {
        port = (int) portObj;
      }
    }
    try {
      return new IoTDBSchema(host, port, userName, password, flavor);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }
}
