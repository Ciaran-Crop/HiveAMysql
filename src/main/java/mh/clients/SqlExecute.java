package mh.clients;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * mysql 数据库操作类
 * @author ciaran
 */
public class SqlExecute {
    private static final Logger LOG = LogManager.getLogger(SqlExecute.class.getName());
    public static String sqlPass;
    public static String hivePass;
    /**
     * 一个 预备命令 方法
     * @param sql 命令
     * @return PreparedStatement Statement
     * @throws SQLException sql
     * @throws ClassNotFoundException notfound
     */
    public static PreparedStatement createStatement(String sql) throws SQLException, ClassNotFoundException {
        Connection con = ConnectionUtil.getSqlConnection(sqlPass);
        return con.prepareCall(sql);
    }
    public static Statement createHiveStatement() throws SQLException, ClassNotFoundException{
        Connection con = ConnectionUtil.getHiveConnection(hivePass);
        return con.createStatement();
    }
    public static Statement createSqlStatement() throws SQLException, ClassNotFoundException{
        Connection con = ConnectionUtil.getSqlConnection(sqlPass);
        return con.createStatement();
    }
    /**
     * 展示 mysql database 下的tables
     * @return List<String> 列表
     * @throws SQLException sql
     * @throws ClassNotFoundException notfound
     */
    public static List<String> getTables() throws SQLException, ClassNotFoundException {
         List<String> listTables = new ArrayList<>();
         LOG.info("执行 show tables ...");
         ResultSet rs =  createStatement("show tables").executeQuery();
         while(rs.next()){
             listTables.add(rs.getString(1));
         }
         LOG.info("获取 tables 成功 ");
         return listTables;
    }

    /**
     * 展示 hive database 下的tables
     * @return List<String> 列表
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static List<String> getTables2() throws SQLException, ClassNotFoundException {
        List<String> listTables = new ArrayList<>();
        LOG.info("执行 show tables ...");
        Statement hiveStatement = createHiveStatement();
        ResultSet rs = hiveStatement.executeQuery("show tables");
        while(rs.next()){
            listTables.add(rs.getString(1));
        }
        LOG.info("获取 tables 成功 ");
        return listTables;
    }

    /**
     * mysql列表内容
     * @param tableName 表名
     * @return 二维表
     * @throws SQLException sql
     * @throws ClassNotFoundException notfound
     */
    public static List<List<Object>> getTablesContext(String tableName) throws SQLException, ClassNotFoundException {
        List<List<Object>> listAll = new ArrayList<>();
        LOG.info("执行 select * from " + tableName + " ...");
        ResultSet rs = createStatement("select * from " + tableName).executeQuery();
        int columnCount = rs.getMetaData().getColumnCount();
        while(rs.next()){
            List<Object> subList = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                subList.add(rs.getObject(i));
            }
            listAll.add(subList);
        }
        LOG.info("获取 " + tableName + "数据成功");
        return listAll;
    }
    /**
     * hive列表内容
     * @param tableName 表名
     * @return 二维表
     * @throws SQLException sql
     * @throws ClassNotFoundException notfound
     */
    public static List<List<Object>> getTablesContext2(String tableName) throws SQLException, ClassNotFoundException {
        List<List<Object>> listAll = new ArrayList<>();
        LOG.info("执行 select * from " + tableName + " ...");
        Statement hiveStatement = createHiveStatement();
        ResultSet rs = hiveStatement.executeQuery("select * from " + tableName);
        int columnCount = rs.getMetaData().getColumnCount();
        while(rs.next()){
            List<Object> subList = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                subList.add(rs.getObject(i));
            }
            listAll.add(subList);
        }
        LOG.info("获取 " + tableName + "数据成功");
        return listAll;
    }

    public static void loadToHive(String tableName) throws IOException, SQLException, ClassNotFoundException {
        FileSystem fs = ConnectionUtil.getHdfsFileSystem();
        String ciaran = "/ciaran/";
        String csv = ".txt";
        if(!fs.exists(new Path(ciaran))){
            fs.mkdirs(new Path(ciaran));
        }
        if(fs.exists(new Path(ciaran + tableName + csv))){
            fs.delete(new Path(ciaran + tableName + csv),true);
        }
        LOG.info("路径正确");
        LOG.info("上传文件中 ..." + "table/" + tableName + csv);
        fs.copyFromLocalFile(new Path("table/" + tableName + csv),new Path(ciaran));
        createTable(tableName);
        LOG.info("上传文件成功 " + "table/" + tableName + csv);
        String sql = "LOAD DATA INPATH" + "'/ciaran/" + tableName + ".txt' overwrite INTO TABLE " + tableName;
        LOG.info("执行 " + sql + " ...");
        createHiveStatement().execute(sql);
    }

    public static void loadToMysql(String tableName,List<List<Object>> listContext) throws IOException, SQLException, ClassNotFoundException {
        createTable2(tableName);
        Statement sqlStatement = createSqlStatement();
        for (List<Object> a:
                listContext) {
            StringBuilder sb = new StringBuilder();
            sb.append("insert into " + tableName + " values(");
            for (Object aa :
                    a) {
                sb.append("\"").append(aa).append("\"").append(",");
            }
            sb.deleteCharAt(sb.length()-1);
            sb.append(')');
            LOG.info(sb.toString());
            sqlStatement.execute(sb.toString());

        }
    }

    /**
     * 创建 hive table
     * @param tableName
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void createTable(String tableName) throws SQLException, ClassNotFoundException {
        LOG.info("正在创建表" + tableName);
        ResultSet rs = createStatement("desc " + tableName).executeQuery();
        List<String> fields = new ArrayList<>();
        while(rs.next()){
            fields.add(rs.getString(1));
        }
        StringBuilder sql = new StringBuilder("create table if not exists " + tableName + "(\n");
        for (String field : fields) {
            String sub = "\t" + field + " String,\n";
            sql.append(sub);
        }
        sql = new StringBuilder(sql.substring(0, sql.length() - 2));
        sql.append(")\n");
        sql.append("row format delimited fields terminated by ','");
        LOG.info("执行 " + sql);
        createHiveStatement().execute(sql.toString());
    }
    /**
     * 创建 mysql table
     * @param tableName
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void createTable2(String tableName) throws SQLException, ClassNotFoundException {
        LOG.info("正在创建表" + tableName);
        Statement hiveStatement = createHiveStatement();
        ResultSet rs = hiveStatement.executeQuery("desc " + tableName);
        List<String> fields = new ArrayList<>();
        while(rs.next()){
            fields.add(rs.getString(1));
        }
        StringBuilder sql = new StringBuilder("create table if not exists " + tableName + "(\n");
        for (String field : fields) {
            String sub = "\t" + field + " Text,\n";
            sql.append(sub);
        }
        sql = new StringBuilder(sql.substring(0, sql.length() - 2));
        sql.append(")ENGINE=InnoDB DEFAULT CHARSET=utf8;\n");
        LOG.info("执行 " + sql);
        createSqlStatement().execute(sql.toString());
    }

}
