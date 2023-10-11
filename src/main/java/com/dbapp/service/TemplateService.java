package com.dbapp.service;

import com.dbapp.templates.AggTemplate;
import com.dbapp.templates.AggTemplates;
import com.dbapp.templates.QueryTemplate;
import com.dbapp.templates.QueryTemplates;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.yaml.snakeyaml.Yaml;

import javax.annotation.PostConstruct;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 模板服务
 *
 * @author yangmenglong
 * @date 2023/9/26
 */
@Service
@Slf4j
public class TemplateService {
    private static final String AGG_TEMPLATE_FILE = "aggs.yml";
    private static final String QUERY_TEMPLATE_FILE = "query-template.yml";

    private QueryTemplates queryTemplates;

    @PostConstruct
    public void init() {
        String path = Thread.currentThread().getContextClassLoader().getResource("").getPath();

        Yaml yaml = new Yaml();
        try {
            log.info("开始载入查询及聚合模板");
            queryTemplates = yaml.loadAs(new FileInputStream(path + QUERY_TEMPLATE_FILE), QueryTemplates.class);

            AggTemplates aggTemplates = yaml.loadAs(new FileInputStream(path + AGG_TEMPLATE_FILE), AggTemplates.class);

            List<AggTemplate> aggs = aggTemplates.getAggs();
            Map<String, AggTemplate> aggMap = aggs.stream().collect(Collectors.toMap(AggTemplate::getKey, agg -> agg));

            queryTemplates.getTemplates().forEach(queryTemplate -> {
                List<String> queryAggs = queryTemplate.getAggs();
                if (queryAggs != null) {
                    List<AggTemplate> aggTemplateList = new ArrayList<>(queryAggs.size());
                    queryAggs.forEach(key -> aggTemplateList.add(aggMap.get(key)));
                    queryTemplate.setAggTemplates(aggTemplateList);
                }
            });

            log.info("模板载入完成，查询模板：{}，聚合模板：{}", queryTemplates.getTemplates().size(), aggs.size());
        } catch (FileNotFoundException e) {
            log.error("模板载入异常", e);
        }
    }

    public QueryTemplates getQueryTemplates() {
        return queryTemplates;
    }

    public String replaceParams(String sql, QueryTemplate queryTemplate, Map<String, Object> data) {
        String pattern = "\\$\\{(\\w+)}";

        Pattern regexPattern = Pattern.compile(pattern);

        String query = sql;

        // 创建 Matcher 对象
        Matcher matcher = regexPattern.matcher(query);

        String paramType = queryTemplate.getParamType();

        switch (paramType) {
            case "dynamic":
                while (matcher.find()) {
                    String group = matcher.group();
                    String key = group.substring(2, group.length() - 1);
                    String value = String.valueOf(data.get(key));
                    query = query.replaceFirst(pattern, value);
                }
                break;
            case "range":
                List<Object> params = new ArrayList<>(queryTemplate.getParams());
                Random random = new Random();
                while (matcher.find()) {
                    int index = random.nextInt(params.size());
                    query = query.replaceFirst(pattern, String.valueOf(params.get(index)));
                    params.remove(index);
                }
                break;
            default:
                log.info("未知参数类型：{}", paramType);
        }

        return query;
    }
}
