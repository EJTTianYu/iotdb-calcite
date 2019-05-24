package org.apache.iotdb.calcite;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

public class IoTDBCalciteDemo {

  public static SchemaPlus getRootSchema() throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.calcite.jdbc.Driver");

    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
//    info.setProperty("remarks","true");
//    info.setProperty("parserFactory","org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl#FACTORY");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    System.out.println(calciteConnection.getProperties());
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    return rootSchema;
  }

  public static void IoTDBConnectionTest(String path)
      throws ClassNotFoundException, IOException, SQLException {
    System.out.println("reading config from path: " + path);
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    InputStream inputStream = new FileInputStream(path);
    Properties properties = new Properties();
    properties.load(inputStream);
    String user = properties.getProperty("user", "root");
    String pass = properties.getProperty("pass", "root");
    String IP = properties.getProperty("IP");
    String Port = properties.getProperty("Port");
    String urlFormat = "jdbc:iotdb://%s:%s/";
    System.out.println("connect " + String.format(urlFormat, IP, Port));
    Connection connection = DriverManager
        .getConnection(String.format(urlFormat, IP, Port), user, pass);
    Statement statement = connection.createStatement();
    DatabaseMetaData databaseMetaData = connection.getMetaData();
//    ResultSet resultSet=databaseMetaData.getTables(null,null,"%",new String[]{"%"});
    boolean hasResultSet = statement.execute("select * from root.sgcc.wf03.wt01");
//    boolean hasResultSet = statement.execute("show timeseries root.android");
    if (hasResultSet) {
      ResultSet resultSet = statement.getResultSet();
      outputResult(resultSet);
    }
  }

  public static void outputResult(ResultSet resultSet) throws SQLException {
    System.out.println("--------------------------");
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    for (int i = 0; i < columnCount; i++) {
      System.out.print(metaData.getColumnLabel(i + 1) + " ");
    }
    System.out.println();
    while (resultSet.next()) {
      for (int i = 1; ; i++) {
        System.out.print(resultSet.getString(i));
        if (i < columnCount) {
          System.out.print(", ");
        } else {
          System.out.println();
          break;
        }
      }
    }
    System.out.println("--------------------------");
  }


  public static void main(String[] args) throws Exception {
//    SchemaPlus rootSchema=getRootSchema();
//    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
//    BasicDataSource dataSource=new BasicDataSource();
//    dataSource.setUrl("jdbc:iotdb://192.168.130.7:6667/");
//    dataSource.setUsername("root");
//    dataSource.setPassword("root");
    IoTDBConnectionTest("calcite/src/main/resources/connection.properties");
  }
}
