package com.atguigu.gmalllogger.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class LoggerController {
    @RequestMapping("test")
    public String test1(){
        System.out.println("success");
        return "success";
    }
}
