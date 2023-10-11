package com.dbapp.templates;

import lombok.Data;

import java.util.List;

/**
 * 查询模板
 *
 * @author yangmenglong
 * @date 2023/9/26
 */
@Data
public class QueryTemplate {
    private String name;
    private String query;
    private String paramType;
    private String order;
    private List<Object> params;
    private int size = 10;
    private List<String> aggs;
    private List<AggTemplate> aggTemplates;
}
