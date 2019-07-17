package com.zhiboclub.ycapp.test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;


public class test_hashset {

    //监控的主播ID
    private static HashSet<String> MonLiveId = new HashSet<>();

    //正在直播的观看用户ID
    private static HashSet<String> AccessingUser = new HashSet<>();

    //已经访问过正在直播的观看用户ID
    private static HashSet<String> AccessedUser = new HashSet<>();

    //历史访问直播间ID
    private static List<String> HistoryAccess = new ArrayList<>();

    //粉丝离开去的直播间ID
    private static List<String> DepartureAccess = new ArrayList<>();

    static {
        MonLiveId.add("230703875007");
        MonLiveId.add("230734135125");
        MonLiveId.add("230577279604");

        AccessedUser.add("230577279604@123");
        AccessedUser.add("230734135125@123");
        AccessingUser.add("230734135121@123");
    }


    public static void main(String[] args) {
        HashSet<String> tmp = new HashSet<>();
        AccessedUser.add("12");
        for (String i : AccessedUser){
            System.out.println(i);
        }
    }
}
