package com.zhiboclub.ycapp.test;

import java.util.HashMap;
import java.util.Map;

public class test_map {
    public static void main(String[] args) {
        HashMap<String,Integer> map = new HashMap<>();

        map.put("a@b",2);

        if(map.containsKey("a@b"))
            if(map.get("a@b")==1){
                System.out.println(1);
            }else {
                map.put("a@b",2);
            }
        else {
            map.put("a@c",1);
        }
        System.out.println(map);
    }
}
