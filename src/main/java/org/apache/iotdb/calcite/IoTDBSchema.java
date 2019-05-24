package org.apache.iotdb.calcite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.iotdb.calcite.IoTDBTable.Flavor;

public class IoTDBSchema extends AbstractSchema {

  private final String host;
  private final int port;
  private final String userName;
  private final String password;
  private static Connection connection;
  private Map<String, Table> tableMap;
  private final IoTDBTable.Flavor flavor;

  public static Connection getIoTDBConn() {
    return connection;
  }

  public IoTDBSchema(String host, int port, String userName, String password,
      IoTDBTable.Flavor flavor)
      throws ClassNotFoundException, SQLException {
    super();
    this.host = host;
    this.port = port;
    this.userName = userName;
    this.password = password;
    this.flavor = flavor;

    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    String urlFormat = "jdbc:iotdb://%s:%s/";
    String url = String.format(urlFormat, host, port);
    connection = DriverManager.getConnection(url, userName, password);
    System.out.println("Connect IoTDB successfully");
  }

  @Override
  protected Map<String, Table> getTableMap() {
    if (tableMap == null) {
      tableMap = createTableMap();
    }
    return tableMap;
  }

  public Map<String, Table> createTableMap() {
    Map<String, Table> tableMap = new HashMap<>();
    try {
      Statement statement = connection.createStatement();
      boolean hasResultSet = statement.execute("show storage group");
      if (hasResultSet) {
        ResultSet resultSet = statement.getResultSet();
        while (resultSet.next()) {
          String tmpTableName = resultSet.getString(1);
          final Table tmpTable = createTable(tmpTableName);
          tableMap.put(tmpTableName, tmpTable);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return tableMap;
  }

  private Table createTable(String storageGroupName) {
    switch (flavor) {
      case SCANNABLE:
        return new IoTDBScannableTable(storageGroupName);
      case PROFIL:
        return new IoTDBProjectFilterTable(storageGroupName);
      default:
        throw new AssertionError("Unknown flavor " + this.flavor);
    }
  }
}
