package com.dbapp.controller;

import com.dbapp.service.DorisService;
import com.dbapp.service.EsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * doris查询测试
 *
 * @author yangmenglong
 * @date 2023/9/25
 */
@RestController
@RequestMapping("/pt")
public class PTController {

    @Autowired
    private DorisService dorisService;

    @Autowired
    private EsService esService;

    @PostMapping("/all/{count}")
    public String startAllPT(@PathVariable int count) {
        Thread dorisThread = new Thread(() -> dorisService.startPT(count), "doris-pt");
        dorisThread.start();
        Thread esThread = new Thread(() -> esService.startPT(count), "es-pt");
        esThread.start();
        return "started";
    }

    @PostMapping("/doris/{count}")
    public String startDorisPT(@PathVariable int count) {
        boolean success = dorisService.startPT(count);
        return success ? "true" : "false";
    }

    @PostMapping("/es/{count}")
    public String startEsPT(@PathVariable int count) {
        boolean success = esService.startPT(count);
        return success ? "true" : "false";
    }
}
