package mh.clients;

import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class hiveToSqlOneTest {
    @Test
    public void oneTest() throws SQLException, IOException, ClassNotFoundException {
        SqlExecute.sqlPass = "ciaran9262_";
        SqlExecute.hivePass = "root";
        Clients.hiveToMysql("hive_mysql_test","hive_mysql_test","192.168.150.201");
    }
    @Test
    public void testConnection() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        String url = "jdbc:" + "mysql" + "://" + "192.168.150.201" + ":3306" + "/" + "hive_mysql_test?useSSL=false";
        Connection connection = DriverManager.getConnection(url, "root", "ciaran9262_");
    }
}
