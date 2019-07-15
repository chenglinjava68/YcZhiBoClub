package com.zhiboclub.ycapp.test;

import com.zhiboclub.ycapp.DBopts.PGCopyInUtils;
import org.apache.hadoop.mapreduce.jobhistory.Events;

import java.sql.Connection;
import java.sql.DriverManager;  
import java.sql.ResultSet;  
import java.sql.SQLException;  
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;

public class test_gp_conn {  
    public static void main(String[] args) throws SQLException {
//        try {
//            Class.forName("org.postgresql.Driver");
//            Connection db = DriverManager.getConnection("jdbc:postgresql://gp-bp15a5elb583fb1n8o.gpdb.rds.aliyuncs.com:3432/testdb","yicai","yicai@123");
//            Statement st = db.createStatement();
//            ResultSet rs = st.executeQuery("select * from test;");
//            while (rs.next()) {
//                System.out.print(rs.getString(1));
//                System.out.print("    |    ");
//                System.out.println(rs.getString(2));
//            }
//            rs.close();
//            st.close();
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
        String uid="134902307";
        String sql ="select DISTINCT \"liveId\",\"anchorId\",\"userId\",title,\"startTime\" from (select * from events WHERE \"userId\"="+uid+")a ORDER BY a.\"userId\", a.\"startTime\"";
        System.out.println();
        ResultSet rs = PGCopyInUtils.getinstance().query(sql);
        List<Events> tbInfos = PGCopyInUtils.getinstance().resultSetToList(rs);
        System.out.println(tbInfos);
    }  
}