package com.zhiboclub.ycapp.test;

import java.util.HashMap;
import java.util.Map;

public class test_map {
    public static void main(String[] args) {
        Map<String,String> map = new HashMap<>();

        map.put("a","a");
        map.put("a","b");
        System.out.println(map);
    }
}
