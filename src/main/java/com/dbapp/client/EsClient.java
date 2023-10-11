package com.dbapp.client;

import com.dbapp.config.EsConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * es客户端
 *
 * @author yangmenglong
 * @date 2023/9/25
 */
@Component
@Slf4j
public class EsClient {

    @Autowired
    private EsConfig esConfig;

    private RestHighLevelClient highLevelClient;

    @PostConstruct
    public void init() {
        log.info("es集群连接参数：{}", esConfig);

        HttpHost httpHost = new HttpHost(esConfig.getIp(), esConfig.getPort());
        RestClientBuilder builder = RestClient.builder(httpHost);
        builder.setHttpClientConfigCallback(cfg -> {
            cfg.setKeepAliveStrategy((x, y) -> 1000L);
            return cfg;
        }).setRequestConfigCallback(requestConfigBuilder -> {
            requestConfigBuilder.setConnectionRequestTimeout(esConfig.getConnectionRequestTimeout());
            requestConfigBuilder.setConnectTimeout(esConfig.getConnectTimeout());
            requestConfigBuilder.setSocketTimeout(esConfig.getSocketTimeout());
            return requestConfigBuilder;
        });

        highLevelClient = new RestHighLevelClient(builder);

        log.info("es客户端构建完成");
    }

    public RestHighLevelClient getRestClient() {
        return highLevelClient;
    }
}
