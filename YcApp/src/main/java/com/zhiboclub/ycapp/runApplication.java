package com.zhiboclub.ycapp;

import com.zhiboclub.ycapp.kafka.OptionsCli;

public class runApplication {
    public static void main(String[] args){
        new OptionsCli().getConf2Cli(args);
    }
}
