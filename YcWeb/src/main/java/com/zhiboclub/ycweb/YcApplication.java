package com.zhiboclub.ycweb;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.zhiboclub.ycweb.mapper")
public class YcApplication {
    public static void main(String[] args) {
        SpringApplication.run(YcApplication.class, args);
    }
}
