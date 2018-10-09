package ctg.workflow.sqlexporter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {

    private static String URL = "jdbc:mysql://" + DBConfig.ip + ":" + DBConfig.port + "/" + DBConfig.dbName
            + "?serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8";

    /**
     * 提供getConnection()方法
     * @return Connection
     */
    public static Connection getConnection(){
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(URL, DBConfig.username, DBConfig.password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return conn;
    }

}

