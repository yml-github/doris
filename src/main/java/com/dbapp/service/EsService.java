package com.dbapp.service;

import com.alibaba.fastjson.JSONObject;
import com.dbapp.client.EsClient;
import com.dbapp.config.EsConfig;
import com.dbapp.dic.DicLoader;
import com.dbapp.exception.BaasELException;
import com.dbapp.result.PTResult;
import com.dbapp.templates.AggTemplate;
import com.dbapp.templates.QueryTemplate;
import com.dbapp.templates.QueryTemplates;
import com.dbapp.utils.TimeUtil;
import com.dbapp.visit.es.EsQueryGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.util.datetime.FastDateFormat;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * es查询服务
 *
 * @author yangmenglong
 * @date 2023/9/26
 */
@Service
@Slf4j
public class EsService {

    @Autowired
    private EsConfig esConfig;

    @Autowired
    private EsClient esClient;

    @Autowired
    private TemplateService templateService;

    @Autowired
    private RecordService recordService;

    @Autowired
    private LocalDataService localDataService;

    @Value("${example.data.type}")
    private String exampleDataType;

    public boolean startPT(int count) {
        log.info("开始执行ES查询测试，总轮询次数：{}", count);
        RestHighLevelClient restClient = esClient.getRestClient();

        QueryTemplates queryTemplates = templateService.getQueryTemplates();
        List<QueryTemplate> templates = queryTemplates.getTemplates();
        List<PTResult> ptResults = new ArrayList<>();
        try {
            for (int i = 1; i <= count; i++) {
                List<Map<String, Object>> exampleData;
                if ("local".equals(exampleDataType)) {
                    exampleData = localDataService.getESLocalDataExample();
                } else if ("random".equals(exampleDataType)) {
                    exampleData = getExampleData();
                } else {
                    exampleData = localDataService.getDorisCachedExampleData(i);
                }

                int exampleIndex = 0;

                for (QueryTemplate queryTemplate : templates) {
                    log.info("开始第{}次执行场景：{}", i, queryTemplate.getName());
                    String query = queryTemplate.getQuery();
                    String replaceQuery = templateService.replaceParams(query, queryTemplate, exampleData.get(exampleIndex++));
                    String esQuery = transEsQuery(replaceQuery);
                    log.info("aiql转esQuery: {}", esQuery);
                    SearchRequest searchRequest = buildRequest(esConfig.getStartTime(), esConfig.getEndTime(), esQuery, queryTemplate.getSize());
                    log.info("query: {}, dsl: {}", replaceQuery, searchRequest);
                    PTResult queryResult = executeEsQuery(restClient, searchRequest, replaceQuery, queryTemplate.getName(), null, i);
                    ptResults.add(queryResult);
                    List<AggTemplate> aggTemplates = queryTemplate.getAggTemplates();
                    if (aggTemplates != null) {
                        for (AggTemplate aggTemplate : aggTemplates) {
                            SearchRequest aggRequest = buildRequest(null, null, esQuery, 0);
                            completeAggDsl(aggRequest, aggTemplate);
                            log.info("聚合场景：{}, agg dsl: {}", aggTemplate.getKey(), aggRequest);
                            PTResult aggResult = executeEsQuery(restClient, aggRequest, replaceQuery, queryTemplate.getName(), aggTemplate.getKey(), i);
                            ptResults.add(aggResult);
                        }
                    }
                }
            }

            log.info("es查询测试结束，结果：{}", ptResults);

            recordService.record(ptResults, "es");
            recordService.recordExcel(ptResults, "es", count);
        } catch (Exception e) {
            log.error("es查询执行失败", e);
            return false;
        }
        return true;
    }

    private void completeAggDsl(SearchRequest request, AggTemplate aggTemplate) {
        SearchSourceBuilder source = request.source();
        switch (aggTemplate.getKey()) {
            case "Count":
                source.size(0);
                break;
            case "TopN":
                TermsAggregationBuilder topN = AggregationBuilders.terms("topN").field(aggTemplate.getField()).size(500);
                source.aggregation(topN);
                break;
            case "DateHistogram":
                DateHistogramAggregationBuilder dateHistogram = AggregationBuilders.dateHistogram("dateHistogram").field(aggTemplate.getField()).fixedInterval(DateHistogramInterval.minutes(5));
                source.aggregation(dateHistogram);
                break;
            default:
                log.error("未知的聚合方式：{}", aggTemplate.getKey());
        }
    }

    private PTResult executeEsQuery(RestHighLevelClient client, SearchRequest request, String query, String name, String aggKey, int currentCount) throws IOException {
        long start = System.currentTimeMillis();
        log.info("执行开始时间：{}", FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(start));

        PTResult ptResult;

        boolean success = true;
        String errorMsg = null;
        if (StringUtils.isNotBlank(aggKey)) {
            ptResult = new PTResult(name + " - " + aggKey, query, start, currentCount);
        } else {
            ptResult = new PTResult(name, query, start, currentCount);
        }

        try {
            client.search(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            success = false;
            errorMsg = e.getMessage();
            log.error("es查询执行异常", e);
        }

        long end = System.currentTimeMillis();
        long cost = end - start;
        log.info("执行结束时间：{}，耗时：{}ms", FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(end), cost);

        ptResult.setEnd(end);
        ptResult.setCost(cost);
        ptResult.setSuccess(success);
        ptResult.setErrorMsg(errorMsg);
        return ptResult;
    }

    private String transEsQuery(String query) throws BaasELException {
        EsQueryGenerator esQueryGenerator = new EsQueryGenerator(DicLoader.getDic());
        //转换成es查询语句
        JSONObject transQuery = esQueryGenerator.visit(query);
        return transQuery.getJSONObject("query").toJSONString();
    }

    private SearchRequest buildRequest(String startTime, String endTime, String esQuery, int size) throws ParseException {
        SearchRequest searchRequest = new SearchRequest(esConfig.getIndex());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String start = null;
        String end = null;
        if (StringUtils.isNotBlank(startTime)) {
            start = TimeUtil.getCollectorReceiptTimeZ(startTime, 8);
        }
        if (StringUtils.isNotBlank(endTime)) {
            end = TimeUtil.getCollectorReceiptTimeZ(endTime, 8);
        }
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().filter(QueryBuilders.rangeQuery("@timestamp").from(start).to(end));
        if (StringUtils.isNotBlank(esQuery)) {
            queryBuilder.filter(QueryBuilders.wrapperQuery(esQuery));
        }
        searchSourceBuilder.trackTotalHits(true).size(size)
                .query(queryBuilder);
        if (size > 0) {
            searchSourceBuilder.sort("collectorReceiptTime", SortOrder.DESC);
        }

        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }


    private List<Map<String, Object>> getExampleData() throws ParseException, IOException {
        List<Map<String, Object>> examples = new ArrayList<>(50);

        SearchRequest request = new SearchRequest(esConfig.getIndex());
        SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.size(50).query(QueryBuilders.functionScoreQuery(QueryBuilders.rangeQuery("collectorReceiptTime")
                    .gte(esConfig.getStartTime())
                    .lte(esConfig.getEndTime()), ScoreFunctionBuilders.randomFunction())).trackTotalHits(true);

        request.source(builder);

        log.info("随机获取样例数据DSL: {}", builder);

        SearchResponse response = esClient.getRestClient().search(request, RequestOptions.DEFAULT);

        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit searchHit : hits) {
            examples.add(searchHit.getSourceAsMap());
        }

        log.info("总数据量：{}, 获取样例数据：{}条", response.getHits().getTotalHits().value, examples.size());

        return examples;
    }
}
