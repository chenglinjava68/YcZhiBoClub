package com.zhiboclub.ycapp.test;

import javax.swing.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class test {

    public static void main(String[] args) {

        List<String> list = new ArrayList<>();
        list.add("S1562894979001&230588970375");
        list.add("S1562894978005&230588970375");
        list.add("S1562894977003&230588970375");
        list.add("S1562894979004&230588970375");
        list.add("S1562894979006&230588970375");
        list.add("S1562894979007&230588970375");
        list.add("S1562894979002&230588970375");
        Collections.sort(list);
        System.out.println(list);
    }
}

