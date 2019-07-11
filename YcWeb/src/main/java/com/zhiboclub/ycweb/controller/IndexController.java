package com.zhiboclub.ycweb.controller;

import com.zhiboclub.ycweb.utils.JsonData;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class IndexController {

    @GetMapping("/index")
    public Object Index(){
        return JsonData.buildSuccess("successful");
    }
}
