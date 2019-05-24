import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.iotdb.calcite.IoTDBCalciteDemo;
import org.apache.iotdb.calcite.IoTDBSchema;
import org.apache.iotdb.calcite.IoTDBSchemaFactory;
import org.apache.iotdb.calcite.IoTDBTable;
import org.apache.iotdb.calcite.IoTDBTable.Flavor;
import org.junit.Test;

public class IoTDBTest {

  private Map<String, Object> getOpMap() throws Exception {
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    InputStream inputStream = new FileInputStream(
        "/Users/tianyu/JavaProject/calcite-iotdb/src/main/resources/connection.properties");
    Properties properties = new Properties();
    properties.load(inputStream);
    String user = properties.getProperty("user", "root");
    String pass = properties.getProperty("pass", "root");
    String IP = properties.getProperty("IP");
    String Port = properties.getProperty("Port");
    Map<String, Object> operandMap = new HashMap<>();
    operandMap.put("userName", user);
    operandMap.put("password", pass);
    operandMap.put("host", IP);
    operandMap.put("port", Port);
    operandMap.put("flavor", "profil");
    return operandMap;
  }

  private SchemaPlus getRootSchema() throws Exception {
    Class.forName("org.apache.calcite.jdbc.Driver");
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    return rootSchema;
  }

  @Test
  public void ConnectionTest() throws Exception {
    IoTDBSchema ioTDBSchema = (IoTDBSchema) IoTDBSchemaFactory.INSTANCE
        .create(getRootSchema(), "hr", getOpMap());
    Map<String, Table> ioTDBtables = ioTDBSchema.createTableMap();
    System.out.println(ioTDBtables);
  }

  @Test
  public void IoTDBAdapterTest() throws Exception {
    Class.forName("org.apache.calcite.jdbc.Driver");
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    Map<String, Object> opMap = getOpMap();
    Schema ioTDBSchema = IoTDBSchemaFactory.INSTANCE
        .create(getRootSchema(), "hr", opMap);
    rootSchema.add("hr", ioTDBSchema);

    Statement statement = connection.createStatement();
    boolean hasResult = false;
    hasResult = statement
        .execute(
            "select count(`d1.s1`) from hr.`root.calcite` group by `time`");
    if (hasResult) {
      ResultSet resultSet = statement.getResultSet();
      IoTDBCalciteDemo.outputResult(resultSet);
    }
  }
}
