package com.dbapp.templates;

import lombok.Data;

/**
 * agg模板
 *
 * @author yangmenglong
 * @date 2023/9/26
 */
@Data
public class AggTemplate {
    private String name;
    private String key;
    private String prefix;
    private String suffix;
    private String field;
}
