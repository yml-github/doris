package com.dbapp.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * es连接配置
 *
 * @author yangmenglong
 * @date 2023/9/25
 */
@Component
@ConfigurationProperties(prefix = "elastic")
@Data
public class EsConfig {
    private String ip;
    private int port;
    private String index;
    private int connectTimeout;
    private int socketTimeout;
    private int connectionRequestTimeout;
    private String startTime;
    private String endTime;
}
