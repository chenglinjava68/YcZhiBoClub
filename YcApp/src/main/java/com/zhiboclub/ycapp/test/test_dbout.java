package com.zhiboclub.ycapp.test;

import com.zhiboclub.ycapp.DBopts.PGCopyInUtils;

import java.io.IOException;
import java.sql.SQLException;

public class test_dbout {

    public static void main(String[] args) {
        try {
            PGCopyInUtils.getinstance().copyToFile("231286367278.csv","(select * from events where \"liveId\"=231286367278 ORDER BY \"createdAt\" desc)");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
