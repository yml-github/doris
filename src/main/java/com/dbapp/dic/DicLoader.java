package com.dbapp.dic;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

/**
 * 字典加载
 *
 * @author yangmenglong
 * @date 2022/11/18
 */
@Slf4j
public class DicLoader {
    private static final String DEFAULT_DIC = "default-dic.properties";

    private static JSONObject dicJson;

    public static JSONObject getDic() {
        if (dicJson != null) {
            return dicJson;
        }
        String path = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String filePath = path + DEFAULT_DIC;
        log.info("加载字典配置，文件路径：{}", filePath);
        Properties properties = new Properties();
        try {
            FileReader reader = new FileReader(filePath);
            properties.load(reader);

            List<String> words = new ArrayList<>(properties.stringPropertyNames());
            words.sort(Comparator.naturalOrder());

            dicJson = new JSONObject();
            for (String word : words) {
                dicJson.put(word, new JSONObject().fluentPut("wordType", properties.getProperty(word)));
            }

            log.info("字典加载完成，总字段数：{}", dicJson.size());
        } catch (Exception e) {
            log.error("字典配置加载异常", e);
            return new JSONObject();
        }
        return dicJson;
    }
}
