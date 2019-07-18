package com.zhiboclub.ycapp.Utils;

import com.alibaba.fastjson.JSON;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ArrayListUtil {

    /**
     * 统计相同的key
     */
    public String List2Map(List<String> list) {
        Map<String, Integer> map = new HashMap<>();
        for (String i : list) {
            if (map.containsKey(i)) {
                map.put(i, map.get(i).intValue() + 1);
            } else {
                map.put(i, 1);
            }

        }
        return JSON.toJSONString(map);
    }

    /**
     * 离开人员统计
     * @param list
     * @param split
     * @param size
     * @return
     */
    public String List2MapWithDepartureAccess(List<String> list, String split, int size) {
        Map<String, Integer> map = new HashMap<>();
        for (String i : list) {
            if (i.split(split).length == size){
                String key = i.split(split)[0] +"@"+ i.split(split)[2];
                if (map.containsKey(key)) {
                    map.put(key, map.get(key).intValue() + 1);
                } else {
                    map.put(key, 1);
                }
            }
        }
        return JSON.toJSONString(map);
    }

    /**
     * 拆分list为新list,并去除连续重复的值
     * @param list 列表:[L1,L2,L3]
     * @return  新列表:[L1@L2,L2@L3,L3]
     */
    public List<String> List2NewList(List<String> list){
        List<String> newList = new ArrayList<>();
        String tmp = "";
        for(String l : list){
            if(tmp == "")
                tmp = l;
            else{
                if( !tmp.equals(l)) {
                    newList.add(tmp + "@" + l);
                    tmp = l;
                }
            }
        }
        newList.add(tmp);
        return newList;
    }
}
