package com.dbapp.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.core.util.datetime.FastDateFormat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.*;

/**
 * 本地固定数据
 *
 * @author yangmenglong
 * @date 2023/10/10
 */
@Service
@Slf4j
public class LocalDataService {

    @Value("${example.data.type}")
    private String exampleDataType;

    @Value("${example.local.dir}")
    private String exampleFilePath;

    @Value("${example.local.data.file}")
    private String exampleFileName;

    @Value("${doris.partition}")
    private String partition;

    private List<List<Map<String, Object>>> dorisExampleDataList = new ArrayList<>();

    private List<List<Map<String, Object>>> localFileDataList = new ArrayList<>();

    @PostConstruct
    public void init() {
        try {
            localFileDataList = readLocalFile();
        } catch (IOException e) {
            log.error("加载本地样例文件异常", e);
        }

    }

    public void recordDorisExampleData(List<Map<String, Object>> examples, int currentCount, int totalCount) throws IOException {
        if ("local-file".equals(exampleDataType)) {
            if (currentCount == 1) {
                log.info("当前数据样例类型：{}，新的压测，清理之前的缓存数据", exampleDataType);
                dorisExampleDataList.clear();
            }
            dorisExampleDataList.add(examples);

            if (currentCount == totalCount) {
                File file = new File(exampleFilePath + partition + "-"
                        + FastDateFormat.getInstance("yyyyMMddHHmm").format(System.currentTimeMillis())
                        + ".data");
                file.createNewFile();
                log.info("记录随机数据到本地文件中，共{}轮样例数据", dorisExampleDataList.size());
                FileWriter fileWriter = new FileWriter(file);
                for (List<Map<String, Object>> data : dorisExampleDataList) {
                    fileWriter.write(JSON.toJSONString(data));
                    fileWriter.write(System.lineSeparator());
                }
                fileWriter.flush();
                fileWriter.close();
            }

            return;
        } else if (!"follow-once".equals(exampleDataType)) {
            if (currentCount == 1) {
                log.info("当前数据样例类型：{}，新的压测，清理之前的缓存数据", exampleDataType);
                dorisExampleDataList.clear();
            }
        }

        dorisExampleDataList.add(examples);
    }

    private List<List<Map<String, Object>>> readLocalFile() throws IOException {
        File file = new File(exampleFilePath + exampleFileName);
        if (file.exists()) {
            FileReader reader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(reader);
            List<List<Map<String, Object>>> localFileDataList = new ArrayList<>();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                JSONArray objects = JSON.parseArray(line);
                List<Map<String, Object>> oneExampleData = new ArrayList<>(objects.size());
                for (Object obj : objects) {
                    JSONObject jsonObject = (JSONObject) obj;
                    oneExampleData.add(jsonObject);
                }
                localFileDataList.add(oneExampleData);
            }
            bufferedReader.close();
            reader.close();
            log.info("加载本地样例文件：{}，共{}轮样例", file.getAbsolutePath(), localFileDataList.size());
            return localFileDataList;
        }
        log.info("本地样例文件不存在：{}", exampleFilePath + exampleFileName);
        return new ArrayList<>();
    }

    public List<Map<String, Object>> getDorisLocalDataExample() {
        List<Map<String, Object>> localDataMapList = new ArrayList<>();
        List<String> dataStrList = Arrays.asList("6289605404065239287 47780 183.63.54.123 187.67.131.183", "6289605404065144055 41764 183.63.54.121 16.3.224.180", "6289605404064424183 46182 183.239.164.216 147.185.61.167", "6289605404064411127 51556 58.251.76.194 90.179.51.161", "6289605404063516151 8146 183.63.54.121 128.125.118.15", "6289605404063296759 47713 120.234.5.194 38.161.81.232", "6289605404062601719 18290 183.63.54.122 64.73.89.211", "6289605404062467831 51455 59.38.212.135 50.252.207.196", "6289605404062530807 54568 183.63.54.117 7.96.51.141", "6289605404061621751 45756 120.234.5.194 108.47.215.53", "6289605404062035447 17487 183.63.54.119 158.19.241.200", "6289605404061711351 56126 120.234.5.194 105.228.58.170", "6289605404061459191 46397 183.63.54.123 44.143.159.202", "6289605404061265399 12601 183.63.54.122 185.188.138.51", "6289605404060167671 16530 120.234.5.194 83.163.180.161", "6289605404059665143 42310 59.38.212.135 118.191.100.80");
        for (String dataStr : dataStrList) {
            String[] s = dataStr.split(" ");
            Map<String, Object> data = new HashMap<>();
            data.put("eventId", s[0]);
            data.put("srcPort", Integer.parseInt(s[1]));
            data.put("srcAddress", s[2]);
            data.put("destAddress", s[3]);
            localDataMapList.add(data);
        }
        return localDataMapList;
    }

    public List<Map<String, Object>> getESLocalDataExample() {
        List<Map<String, Object>> localDataMapList = new ArrayList<>();
        List<String> dataStrList = Arrays.asList("6278023441452552466 47780 183.63.54.123 187.67.131.183", "6278027818145073131 41764 183.63.54.121 16.3.224.180", "6278077430933487595 46182 183.239.164.216 147.185.61.167", "6278026225353555986 51556 58.251.76.194 90.179.51.161", "6278027818149584107 8146 183.63.54.121 128.125.118.15", "6278077430934576875 47713 120.234.5.194 38.161.81.232", "6278077430935377899 18290 183.63.54.122 64.73.89.211", "6278027818150179563 51455 59.38.212.135 50.252.207.196", "6278085869286021650 54568 183.63.54.117 7.96.51.141", "6278086904155768043 45756 120.234.5.194 108.47.215.53", "6278026225460029970 17487 183.63.54.119 158.19.241.200", "6278027818145319403 56126 120.234.5.194 105.228.58.170", "6278086904168370667 46397 183.63.54.123 44.143.159.202", "6278086904156342763 12601 183.63.54.122 185.188.138.51", "6278027818057254379 16530 120.234.5.194 83.163.180.161", "6278086904156614891 42310 59.38.212.135 118.191.100.80");
        for (String dataStr : dataStrList) {
            String[] s = dataStr.split(" ");
            Map<String, Object> data = new HashMap<>();
            data.put("eventId", s[0]);
            data.put("srcPort", Integer.parseInt(s[1]));
            data.put("srcAddress", s[2]);
            data.put("destAddress", s[3]);
            localDataMapList.add(data);
        }
        return localDataMapList;
    }

    public List<Map<String, Object>> getDorisCachedExampleData(int count) {
        while (dorisExampleDataList.size() < count) {
            log.info("doris本地缓存数据尚不存在，等待，轮数：{}", count);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        log.info("获取doris随机数据，用于同比，轮数：{}", count);
        return dorisExampleDataList.get(count - 1);
    }

    public List<List<Map<String, Object>>> getDorisExampleDataList() {
        return dorisExampleDataList;
    }

    public List<List<Map<String, Object>>> getLocalFileDataList() {
        return localFileDataList;
    }

    public void setDorisExampleDataList(List<List<Map<String, Object>>> exampleDataList) {
        this.dorisExampleDataList = exampleDataList;
    }
}
