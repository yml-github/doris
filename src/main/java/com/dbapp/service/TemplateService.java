package com.dbapp.service;

import com.dbapp.templates.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.yaml.snakeyaml.Yaml;

import javax.annotation.PostConstruct;
import java.io.*;
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
    private static final String JOIN_TEMPLATE_FILE = "join-template.yml";
    private static final String DORIS_JOIN_TEMPLATE_SQL = "doris-join-template.sql";
    private static final String CK_JOIN_TEMPLATE_SQL = "ck-join-template.sql";

    private QueryTemplates queryTemplates;
    private QueryTemplates joinTemplates;
    private String dorisJoinTemplate;
    private String ckJoinTemplate;

    @PostConstruct
    public void init() {
        String path = Thread.currentThread().getContextClassLoader().getResource("").getPath();

        Yaml yaml = new Yaml();
        try {
            log.info("开始载入查询及聚合模板");
            queryTemplates = yaml.loadAs(new FileInputStream(path + QUERY_TEMPLATE_FILE), QueryTemplates.class);

            joinTemplates = yaml.loadAs(new FileInputStream(path + JOIN_TEMPLATE_FILE), QueryTemplates.class);

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

            File file = new File(path + DORIS_JOIN_TEMPLATE_SQL);
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                StringBuilder joinTemplate = new StringBuilder();
                while ((line = reader.readLine()) != null) {
                    joinTemplate.append(line);
                }
                dorisJoinTemplate = joinTemplate.toString();
            } catch (IOException e) {
                log.error("doris join模板加载异常", e);
            }

            File ckSQLFile = new File(path + CK_JOIN_TEMPLATE_SQL);
            try (BufferedReader reader = new BufferedReader(new FileReader(ckSQLFile))) {
                String line;
                StringBuilder joinTemplate = new StringBuilder();
                while ((line = reader.readLine()) != null) {
                    joinTemplate.append(line);
                }
                ckJoinTemplate = joinTemplate.toString();
            } catch (IOException e) {
                log.error("doris join模板加载异常", e);
            }

            log.info("模板载入完成，查询模板：{}，聚合模板：{}, join模板：{}", queryTemplates.getTemplates().size(),
                    aggs.size(), joinTemplates.getTemplates().size());
            log.info("doris join模板：{}", dorisJoinTemplate);
            log.info("ck join模板：{}", ckJoinTemplate);
        } catch (FileNotFoundException e) {
            log.error("模板载入异常", e);
        }
    }

    public QueryTemplates getQueryTemplates() {
        return queryTemplates;
    }

    public QueryTemplates getJoinTemplates() {
        return joinTemplates;
    }

    public String getDorisJoinTemplateSql() {
        return dorisJoinTemplate;
    }

    public String getCkJoinTemplateSql() {
        return ckJoinTemplate;
    }

    public String replaceParams(String sql, QueryTemplate queryTemplate, Map<String, Object> data, int order) {
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
            case "order":
                List<Object> orderParams = new ArrayList<>(queryTemplate.getParams());
                int i = order;
                while (matcher.find()) {
                    query = query.replaceFirst(pattern, String.valueOf(orderParams.get(i % orderParams.size())));
                    i++;
                }
                break;
            default:
                log.info("未知参数类型：{}", paramType);
        }

        return query;
    }
}
