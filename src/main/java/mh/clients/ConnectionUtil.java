package mh.clients;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 获取连接的工具类
 *
 * @author ciaran
 */
public class ConnectionUtil {
    private static final String TYPE_MYSQL = "mysql";
    private static final String TYPE_HIVE = "hive2";
    private static final Logger LOG = LogManager.getLogger(ConnectionUtil.class.getName());
    public static String databaseSql;
    public static String databaseHive;
    public static String url;

    public static Connection getSqlConnection(String pass) throws ClassNotFoundException, SQLException {
        LOG.info("连接 mysql " + "数据库" + " ...");
        return getConnection("mysql", url, databaseSql,pass);
    }

    private static Connection getConnection(String type, String url, String database,String pass) throws ClassNotFoundException, SQLException {
        if (TYPE_MYSQL.equals(type)) {
            Class.forName("com.mysql.jdbc.Driver");
            url = "jdbc:" + type + "://" + url + ":3306" + "/" + database + "?useSSL=false";
        } else if (TYPE_HIVE.equals(type)) {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            url = "jdbc:" + type + "://" + url + ":10000" + "/" + database;
        }
        return DriverManager.getConnection(url, "root", pass);
    }

    public static Connection getHiveConnection(String pass) throws SQLException, ClassNotFoundException {
        LOG.info("连接 hive " + "数据库" + " ...");
        return getConnection("hive2", url, databaseHive,pass);
    }

    public static FileSystem getHdfsFileSystem() throws IOException {
        LOG.info("连接 HDFS " + " ...");
        Configuration conf = new Configuration();
        // 云服务器必备
        conf.set("dfs.replication", "1");
        conf.set("dfs.client.use.datanode.hostname", "true");
        System.setProperty("HADOOP_USER_NAME", "root");
        String path = "hdfs://"+ url +":9000/";
        conf.set("fs.defaultFS", path);
        return FileSystem.get(URI.create(path), conf);
    }
}
