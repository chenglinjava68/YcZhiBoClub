package com.zhiboclub.ycapp.DBopts;

import java.io.*;
import java.sql.*;
import java.util.*;

import org.apache.log4j.PropertyConfigurator;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhiboclub.ycapp.Utils.ConfigurationManager;

public class PGCopyInUtils {
    static {
        if (new File(System.getProperty("user.dir") + "/conf/log4j.properties").exists()) {
            PropertyConfigurator.configure(System.getProperty("user.dir") + "/conf/log4j.properties");
        } else if (new File(System.getProperty("user.dir") + "/YcApp/conf/log4j.properties").exists()) {
            PropertyConfigurator.configure(System.getProperty("user.dir") + "/YcApp/conf/log4j.properties");
        } else {
            System.out.println("没有log4j的配置文件，日志打印会存在问题!");
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(PGCopyInUtils.class);

    private static String url = null;
    private static String usr = null;
    private static String psd = null;
    private static String driver = null;
    private static Connection connection = null;
    private static PGCopyInUtils pgutil = null;


    private static String psqlPropertiesFile = "";

    /**
     * 实例化类的对象，实现单例
     *
     * @return
     */
    public static PGCopyInUtils getinstance() {
        if (pgutil == null) {
            pgutil = new PGCopyInUtils();
            pgutil.init();
        }
        return pgutil;
    }

    /**
     * 初始化参数
     */
    public void init() {
        if (new File(System.getProperty("user.dir") + "/conf/postgresql.properties").exists()) {
            psqlPropertiesFile = System.getProperty("user.dir") + "/conf/postgresql.properties";
        } else if (new File(System.getProperty("user.dir") + "/YcApp/conf/postgresql.properties").exists()) {
            psqlPropertiesFile = System.getProperty("user.dir") + "/YcApp/conf/postgresql.properties";
        } else {
            LOG.error("没有指定数据库配置文件，无法连接到数据库，请重试!");
            System.exit(1);
        }
        driver = ConfigurationManager.getInstance().GetValues(psqlPropertiesFile, "postgres.driver", "org.postgresql.Driver");
        url = ConfigurationManager.getInstance().GetValues(psqlPropertiesFile, "postgres.url", "jdbc:postgresql://localhost:5432");
        usr = ConfigurationManager.getInstance().GetValues(psqlPropertiesFile, "postgres.username", "test");
        psd = ConfigurationManager.getInstance().GetValues(psqlPropertiesFile, "postgres.password", "test");
        pgutil.getconn();
    }

    /**
     * 获得数据库连接对象
     */
    public void getconn() {
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, usr, psd);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public long copyFromStream(String buffer, String tableName) throws SQLException, IOException {
        InputStream in = null;
        try {
            CopyManager copyManager = new CopyManager((BaseConnection) connection);
            in = new ByteArrayInputStream(buffer.getBytes());
            String copyIn = "COPY " + tableName + " FROM STDIN DELIMITER AS '\u0001'";
            return copyManager.copyIn(copyIn, in);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static PreparedStatement pstmt = null;
    private static ResultSet rs = null;


    /**
     * 通用增删改
     *
     * @param sql
     * @param values
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public void PGupdate(String sql, Object[] values) throws SQLException, ClassNotFoundException {
        //获取数据库链接
        //预编译
        pstmt = connection.prepareStatement(sql);
        //获取ParameterMetaData()对象
        ParameterMetaData pmd = pstmt.getParameterMetaData();
        //获取参数个数
        int number = pmd.getParameterCount();
        //循环设置参数值
        for (int i = 1; i <= number; i++) {
            pstmt.setObject(i, values[i - 1]);
        }
        pstmt.executeUpdate();
    }


    /**
     * 将表中的数据导出到本地文件
     *
     * @param filePath     文件路径
     * @param tableOrQuery 表名 或者查询语句
     * @throws SQLException SQLException
     * @throws IOException  IOException
     */
    public void copyToFile(String filePath, String tableOrQuery)
            throws SQLException, IOException {
        FileOutputStream fileOutputStream = null;
        try {
            CopyManager copyManager = new CopyManager((BaseConnection) connection);
            fileOutputStream = new FileOutputStream(filePath);
            String copyOut = "COPY " + tableOrQuery + " TO STDOUT with csv header";
            final long line = copyManager.copyOut(copyOut, fileOutputStream);
            LOG.info("本次倒出：" + line + "行数据到文件中");
        } finally {
            if (fileOutputStream != null) {
                try {
                    fileOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    //查询方法
    public ResultSet query(String sql) throws SQLException {
        System.out.println(sql);
        if(connection != null){
            pstmt = connection.prepareStatement(sql);
            rs = pstmt.executeQuery();
        }

        return rs;
    }

    //ResultSet转换成list
    public List resultSetToList(ResultSet rs) throws java.sql.SQLException {
        if (rs == null)
            return Collections.EMPTY_LIST;
        ResultSetMetaData md = rs.getMetaData(); //得到结果集(rs)的结构信息，比如字段数、字段名等
        int columnCount = md.getColumnCount(); //返回此 ResultSet 对象中的列数
        List list = new ArrayList();
        Map rowData;
        while (rs.next()) {
            rowData = new HashMap(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                rowData.put(md.getColumnName(i), rs.getObject(i));
            }
            list.add(rowData);
        }
        return list;
    }

    /**
     * 将文件中的数据导入到数据库中
     *
     * @param filePath  文件路径
     * @param tableName 表名
     * @return long 导入的行数
     * @throws SQLException SQLException
     * @throws IOException  IOException
     */
    public long copyFromFile(String filePath, String tableName) throws SQLException, IOException {
        FileInputStream fileInputStream = null;
        try {
            CopyManager copyManager = new CopyManager((BaseConnection) connection);
            fileInputStream = new FileInputStream(filePath);
            String copyIn = "COPY " + tableName + " FROM STDIN DELIMITER AS '\u0001'";
            return copyManager.copyIn(copyIn, fileInputStream);
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public void PGinsertEventsCount(String sql, Object[] values) throws SQLException, ClassNotFoundException {
        //获取数据库链接
        //预编译
        pstmt = connection.prepareStatement(sql);
        //获取ParameterMetaData()对象
        ParameterMetaData pmd = pstmt.getParameterMetaData();
        //获取参数个数
        int number = pmd.getParameterCount();
        //循环设置参数值
        pstmt.setString(0, values[0].toString());
        pstmt.setString(1, values[1].toString());
        pstmt.setInt(2, Integer.parseInt(values[2].toString()));
        pstmt.setInt(3, Integer.parseInt(values[3].toString()));
        pstmt.setInt(4, Integer.parseInt(values[4].toString()));
        pstmt.setString(5, values[5].toString());
        pstmt.setString(6, values[6].toString());
        pstmt.executeUpdate();
    }


}