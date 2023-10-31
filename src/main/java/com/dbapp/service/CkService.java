package com.dbapp.service;

import com.alibaba.fastjson.JSONObject;
import com.dbapp.dic.DicLoader;
import com.dbapp.result.PTResult;
import com.dbapp.templates.QueryTemplate;
import com.dbapp.templates.QueryTemplates;
import com.dbapp.utils.TreeUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.core.util.datetime.FastDateFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class CkService {

    @Autowired
    private TemplateService templateService;

    @Autowired
    private LocalDataService localDataService;

    @Autowired
    private RecordService recordService;

    @Value("${example.data.type}")
    private String exampleDataType;

    @Value("${clickhouse.url}")
    private String jdbcUrl;

    @Value("${clickhouse.user}")
    private String jdbcUser;

    @Value("${doris.startTime}")
    private String startTime;

    @Value("${doris.endTime}")
    private String endTime;

    @Value("${clickhouse.join.alias}")
    private String alias;

    public boolean startPT(int count) {
        log.info("开始执行ck join查询测试，总轮询次数：{}", count);

        JSONObject dic = DicLoader.getDic();

        QueryTemplates queryTemplates = templateService.getJoinTemplates();
        List<QueryTemplate> templates = queryTemplates.getTemplates();
        List<PTResult> ptResults = new ArrayList<>();
        try (Connection connection = connect()) {
            for (int i = 1; i <= count; i++) {
                List<Map<String, Object>> exampleData;
                if ("local".equals(exampleDataType)) {
                    exampleData = localDataService.getESLocalDataExample();
                } else {
                    exampleData = localDataService.getDorisCachedExampleData(i);
                }

                int exampleIndex = 0;

                for (QueryTemplate queryTemplate : templates) {
                    log.info("开始第{}次执行场景：{}", i, queryTemplate.getName());
                    String query = queryTemplate.getQuery();
                    String replacedSQL = templateService.replaceParams(query, queryTemplate, exampleData.get(exampleIndex++), i);
                    String sql = TreeUtil.transMysqlQuery(replacedSQL, dic);
                    sql = sql.replace("\"", "'");
                    String completeSQL = completeSQL(sql);
                    log.info("query: {}, sql: {}", query, completeSQL);
                    PTResult queryResult = executeSQL(completeSQL, queryTemplate.getName(), i, connection);
                    ptResults.add(queryResult);
                }
            }

            log.info("ck join查询测试结束，结果：{}", ptResults);

            recordService.record(ptResults, "ck");
            recordService.recordExcel(ptResults, "ck", count);
        } catch (Exception e) {
            log.error("ck join查询执行失败", e);
            return false;
        }
        return true;
    }

    private String completeSQL(String whereSQL) {
        String joinSql = templateService.getCkJoinTemplateSql();
        return joinSql.replace("${whereCondition}", whereSQL).replace("alias.", alias);
    }

    private PTResult executeSQL(String sql, String name, int currentCount, Connection connection) {
        long start = System.currentTimeMillis();
        log.info("执行开始时间：{}", FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(start));

        PTResult ptResult;

        boolean success = true;
        String errorMsg = null;
        ptResult = new PTResult(name, sql, start, currentCount);
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
        } catch (Exception e) {
            success = false;
            errorMsg = e.getMessage();
            log.error("ck查询执行异常", e);
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

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, jdbcUser, "");
    }
}
