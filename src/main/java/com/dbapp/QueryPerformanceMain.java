package com.dbapp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 查询性能测试启动入口
 *
 * @author yangmenglong
 * @date 2023/9/25
 */
@SpringBootApplication
public class QueryPerformanceMain {
    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(QueryPerformanceMain.class);
        application.run(args);
    }
}
