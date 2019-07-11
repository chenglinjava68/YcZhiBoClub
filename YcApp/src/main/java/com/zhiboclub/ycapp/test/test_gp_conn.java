package com.zhiboclub.ycapp.test;

import java.sql.Connection;  
import java.sql.DriverManager;  
import java.sql.ResultSet;  
import java.sql.SQLException;  
import java.sql.Statement;  
public class test_gp_conn {  
    public static void main(String[] args) {  
        try {  
            Class.forName("org.postgresql.Driver");  
            Connection db = DriverManager.getConnection("jdbc:postgresql://gp-bp15a5elb583fb1n8o.gpdb.rds.aliyuncs.com:3432/testdb","yicai","yicai@123");  
            Statement st = db.createStatement();  
            ResultSet rs = st.executeQuery("select * from test;");  
            while (rs.next()) {  
                System.out.print(rs.getString(1));  
                System.out.print("    |    ");  
                System.out.println(rs.getString(2));  
            }  
            rs.close();  
            st.close();  
        } catch (ClassNotFoundException e) {  
            e.printStackTrace();  
        } catch (SQLException e) {  
            e.printStackTrace();  
        }  
    }  
}