package mh.clients;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.sql.SQLException;
import java.util.List;

/**
 * 主程序
 * @author ciaran
 */
public class Clients {
    private static final Logger LOG = LogManager.getLogger(Clients.class.getName());
    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
//        mysqlToHive("tmp","tmp","192.168.150.201");
        SqlExecute.sqlPass = "ciaran9262_";
        SqlExecute.hivePass = "root";
        hiveToMysql("video_result","video_result","192.168.150.201");
    }

    public static void mysqlToHive(String databaseSql,String databaseHive,String url) throws SQLException, ClassNotFoundException, IOException {
        ConnectionUtil.databaseSql = databaseSql;
        ConnectionUtil.databaseHive = databaseHive;
        ConnectionUtil.url = url;

        LOG.info("start");
        //获取 列表
        List<String> tables = SqlExecute.getTables();
        // 循环列表 获得 数据
        for (String tableName :
                tables) {
            // 写入数据
            write(tableName);
            // 上传到hive
            SqlExecute.loadToHive(tableName);
        }
    }
    public static void hiveToMysql(String databaseSql,String databaseHive,String url) throws SQLException, ClassNotFoundException, IOException {
        ConnectionUtil.databaseSql = databaseSql;
        ConnectionUtil.databaseHive = databaseHive;
        ConnectionUtil.url = url;
        LOG.info("start");
        //获取 列表
        List<String> tables = SqlExecute.getTables2();
        // 循环列表 获得 数据
        for (String tableName :
                tables) {
            List<List<Object>> listContext = SqlExecute.getTablesContext2(tableName);
            SqlExecute.loadToMysql(tableName,listContext);
        }
    }

    /**
     * 将mysql数据库内容写入本地文件
     * @param tableName 表名，将用表名设置文件名
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     */
    public static void write(String tableName) throws SQLException, ClassNotFoundException, IOException {
        List<List<Object>> listContext = SqlExecute.getTablesContext(tableName);
        String filePath = "table/" + tableName + ".txt";
        LOG.info("写入文件" + filePath + " ...");
        BufferedWriter w = new BufferedWriter(new FileWriter(filePath));
        for (List<Object> a:
        listContext){
            StringBuilder sb = new StringBuilder();
            for (Object aa :
                    a) {
                sb.append(aa).append(",");
            }
            w.append(sb.substring(0,sb.length()-1));
            w.append("\n");
        }
        w.flush();
        LOG.info("写入成功");
    }
    /**
     * 将hive数据库内容写入本地文件
     * @param tableName 表名，将用表名设置文件名
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     */
    public static void write2(String tableName) throws SQLException, ClassNotFoundException, IOException {
        List<List<Object>> listContext = SqlExecute.getTablesContext2(tableName);
        String filePath = "table/" + tableName + ".txt";
        LOG.info("写入文件" + filePath + " ...");
        BufferedWriter w = new BufferedWriter(new FileWriter(filePath));
        for (List<Object> a:
                listContext){
            StringBuilder sb = new StringBuilder();
            for (Object aa :
                    a) {
                sb.append(aa).append(",");
            }
            w.append(sb.substring(0,sb.length()-1));
            w.append("\n");
        }
        w.flush();
        LOG.info("写入成功");
    }

}
