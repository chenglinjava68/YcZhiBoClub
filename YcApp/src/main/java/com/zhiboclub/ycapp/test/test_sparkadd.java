package com.zhiboclub.ycapp.test;

import com.zhiboclub.ycapp.Utils.ArrayListUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class test_sparkadd {

    /**
     * @param args
     */
    public static void main(String[] args) {

        List<String> l=new ArrayList<String>();
        l.add("L1") ;
        l.add("L2") ;
        l.add("L1") ;
        l.add("L1");
        l.add("L5") ;
        System.out.println(new test_sparkadd().List2NewList(l));
    }

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
