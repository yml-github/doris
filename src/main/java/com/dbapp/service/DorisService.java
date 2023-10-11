package com.dbapp.service;

import com.alibaba.fastjson.JSONObject;
import com.dbapp.dic.DicLoader;
import com.dbapp.exception.BaasMonitorException;
import com.dbapp.result.PTResult;
import com.dbapp.templates.AggTemplate;
import com.dbapp.templates.QueryTemplate;
import com.dbapp.templates.QueryTemplates;
import com.dbapp.utils.TreeUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.util.datetime.FastDateFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * doris service
 *
 * @author yangmenglong
 * @date 2023/9/25
 */
@Service
@Slf4j
public class DorisService {

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Value("${doris.table}")
    private String table;

    @Value("${doris.partition}")
    private String partition;

    @Value("${doris.startTime}")
    private String startTime;

    @Value("${doris.endTime}")
    private String endTime;

    @Value("${doris.select.fields}")
    private String selectFields;

    @Autowired
    private TemplateService templateService;

    @Autowired
    private RecordService recordService;

    @Autowired
    private LocalDataService localDataService;

    @Value("${example.data.type}")
    private String exampleDataType;

    @Resource(name = "doris")
    private ExecutorService executor;

    public boolean startMTPT(int threadCount) {
        log.info("开始执行Doris并发查询测试，并发数：{}", threadCount);
        JSONObject dic = DicLoader.getDic();

        jdbcTemplate.setQueryTimeout(300000);

        try {
            List<PTResult> ptResults = new ArrayList<>();
            QueryTemplates queryTemplates = templateService.getQueryTemplates();
            List<QueryTemplate> templates = queryTemplates.getTemplates();

            List<List<Map<String, Object>>> exampleDatas = new ArrayList<>();
            for (int i = 1; i <= threadCount; i++) {
                List<Map<String, Object>> exampleData;
                if ("local".equals(exampleDataType)) {
                    exampleData = localDataService.getDorisLocalDataExample();
                } else if ("random".equals(exampleDataType) || "follow".equals(exampleDataType)) {
                    exampleData = getExampleData(i, threadCount);
                } else {
                    if (localDataService.getDorisExampleDataList().size() < i) {
                        exampleData = getExampleData(i, threadCount);
                    } else {
                        log.info("doris复用第: {}轮数据", threadCount);
                        exampleData = localDataService.getDorisCachedExampleData(i);
                    }
                }
                exampleDatas.add(exampleData);
            }

            int exampleIndex = 0;

            for (QueryTemplate queryTemplate : templates) {
                log.info("开始执行场景：{}", queryTemplate.getName());
                String query = queryTemplate.getQuery();
                List<Future<?>> futures = new ArrayList<>(threadCount);
                for (int i = 0; i < threadCount; i++) {
                    List<Map<String, Object>> exampleData = exampleDatas.get(i);
                    Map<String, Object> example = exampleData.get(exampleIndex);
                    int finalI = i;
                    Future<?> submit = executor.submit(() -> {
                        String replacedSQL = templateService.replaceParams(query, queryTemplate, example);
                        String sql = null;
                        try {
                            sql = TreeUtil.transMysqlQuery(replacedSQL, dic);
                        } catch (BaasMonitorException e) {
                            e.printStackTrace();
                        }
                        sql = sql.replace("\"", "'");
                        String completeSQL = completeQuerySQL(sql, queryTemplate.getSize(), queryTemplate.getOrder());
                        log.info("并发：{}, query: {}, sql: {}", finalI, query, completeSQL);
                        PTResult queryResult = executeSQL(completeSQL, queryTemplate.getName(), null, finalI + 1);
                        ptResults.add(queryResult);
                        List<AggTemplate> aggTemplates = queryTemplate.getAggTemplates();
                        if (aggTemplates != null) {
                            for (AggTemplate aggTemplate : aggTemplates) {
                                String aggSQL = completeAggSQL(sql, aggTemplate);
                                log.info("并发: {}, agg sql: {}", finalI, aggSQL);
                                PTResult aggResult = executeSQL(aggSQL, queryTemplate.getName(), aggTemplate.getKey(), finalI + 1);
                                ptResults.add(aggResult);
                            }
                        }
                    });
                    futures.add(submit);
                }

                exampleIndex++;

                for (Future<?> future : futures) {
                    try {
                        future.get();
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            }

            log.info("doris并发查询测试结束，结果：{}", ptResults);

            recordService.record(ptResults, "doris");
            recordService.recordExcel(ptResults, "doris", threadCount);
        } catch (IOException e) {
            log.error("Doris查询执行异常", e);
        }

        return true;
    }

    public boolean startPT(int count) {
        log.info("开始执行Doris查询测试，总轮询次数：{}", count);
        JSONObject dic = DicLoader.getDic();

        jdbcTemplate.setQueryTimeout(300000);

        try {
            List<PTResult> ptResults = new ArrayList<>();
            QueryTemplates queryTemplates = templateService.getQueryTemplates();
            List<QueryTemplate> templates = queryTemplates.getTemplates();

            for (int i = 1; i <= count; i++) {
                List<Map<String, Object>> exampleData;
                if ("local".equals(exampleDataType)) {
                    exampleData = localDataService.getDorisLocalDataExample();
                } else if ("random".equals(exampleDataType) || "follow".equals(exampleDataType)) {
                    exampleData = getExampleData(i, count);
                } else {
                    if (localDataService.getDorisExampleDataList().size() < i) {
                        exampleData = getExampleData(i, count);
                    } else {
                        log.info("doris复用第: {}轮数据", count);
                        exampleData = localDataService.getDorisCachedExampleData(i);
                    }
                }

                int exampleIndex = 0;

                for (QueryTemplate queryTemplate : templates) {
                    log.info("开始第：{}次执行场景：{}", i, queryTemplate.getName());
                    String query = queryTemplate.getQuery();
                    String replacedSQL = templateService.replaceParams(query, queryTemplate, exampleData.get(exampleIndex++));
                    String sql = TreeUtil.transMysqlQuery(replacedSQL, dic);
                    sql = sql.replace("\"", "'");
                    String completeSQL = completeQuerySQL(sql, queryTemplate.getSize(), queryTemplate.getOrder());
                    log.info("query: {}, sql: {}", query, completeSQL);
                    PTResult queryResult = executeSQL(completeSQL, queryTemplate.getName(), null, i);
                    ptResults.add(queryResult);
                    List<AggTemplate> aggTemplates = queryTemplate.getAggTemplates();
                    if (aggTemplates != null) {
                        for (AggTemplate aggTemplate : aggTemplates) {
                            String aggSQL = completeAggSQL(sql, aggTemplate);
                            log.info("agg sql: {}", aggSQL);
                            PTResult aggResult = executeSQL(aggSQL, queryTemplate.getName(), aggTemplate.getKey(), i);
                            ptResults.add(aggResult);
                        }
                    }
                }
            }

            log.info("doris查询测试结束，结果：{}", ptResults);

            recordService.record(ptResults, "doris");
            recordService.recordExcel(ptResults, "doris", count);
        } catch (BaasMonitorException | IOException e) {
            log.error("Doris查询执行异常", e);
            return false;
        }
        return true;
    }

    private PTResult executeSQL(String sql, String name, String aggKey, int currentCount) {
        long start = System.currentTimeMillis();
        log.info("执行开始时间：{}", FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(start));

        PTResult ptResult;

        boolean success = true;
        String errorMsg = null;
        if (StringUtils.isNotBlank(aggKey)) {
            ptResult = new PTResult(name + " - " + aggKey, sql, start, currentCount);
        } else {
            ptResult = new PTResult(name, sql, start, currentCount);
        }
        try {
            jdbcTemplate.execute(sql);
        } catch (Exception e) {
            success = false;
            errorMsg = e.getMessage();
            log.error("doris查询执行异常", e);
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

    private String completeQuerySQL(String whereSQL, int size, String order) {
        StringBuilder sql = new StringBuilder();
        whereSQL = "(collectorReceiptTime >= \"" + startTime + "\" AND collectorReceiptTime <= \"" + endTime + "\") AND (" + whereSQL + ")";
        sql.append("select ").append(selectFields).append(" from ").append(table).append(" PARTITION(").append(partition).append(") where ").append(whereSQL);
        if (StringUtils.isNotBlank(order)) {
            sql.append(" ORDER BY ").append(order);
        }
        sql.append(" limit ").append(size);
        return sql.toString();
    }

    private String completeAggSQL(String whereSQL, AggTemplate aggTemplate) {
        StringBuilder sql = new StringBuilder();
        whereSQL = "(collectorReceiptTime >= \"" + startTime + "\" AND collectorReceiptTime <= \"" + endTime + "\") AND (" + whereSQL + ")";
        sql.append(aggTemplate.getPrefix()).append(" from ").append(table)
                .append(" PARTITION(").append(partition).append(") where ").append(whereSQL);
        if (StringUtils.isNotBlank(aggTemplate.getSuffix())) {
            sql.append(" ").append(aggTemplate.getSuffix());
        }
        return sql.toString();
    }

    private List<Map<String, Object>> getExampleData(int currentCount, int totalCount) throws IOException {
        if ("local-file".equals(exampleDataType) && localDataService.getLocalFileDataList().size() >= totalCount) {
            log.info("使用本地文件样例数据");
            localDataService.setDorisExampleDataList(localDataService.getLocalFileDataList());
            return localDataService.getLocalFileDataList().get(currentCount);
        } else {
            log.warn("本地文件总轮数小于需要的轮数，不使用");
        }
        String totalSQL = "select count(1) as count from " + table + " PARTITION(" + partition + ")" +
                " where " + "collectorReceiptTime >= \"" + startTime + "\" AND collectorReceiptTime <= \"" + endTime + "\"";
        log.info("Doris获取数据总量：{}", totalSQL);
        List<Map<String, Object>> totalResults = jdbcTemplate.queryForList(totalSQL);
        long total = Long.parseLong(String.valueOf(totalResults.get(0).get("count")));
        String randomSQL = "select " + selectFields + " from " + table + " PARTITION(" + partition + ") " +
                " where " + "collectorReceiptTime >= \"" + startTime + "\" AND collectorReceiptTime <= \"" + endTime + "\" LIMIT 50";
        log.info("Doris随机取数据SQL：{}", randomSQL);
        log.info("Doris展示列：{}", selectFields);
        List<Map<String, Object>> examples = jdbcTemplate.queryForList(randomSQL);

        log.info("总数据量：{}, 获取样例数据：{}条", total, examples.size());

        localDataService.recordDorisExampleData(examples, currentCount, totalCount);

        return examples;
    }

}
