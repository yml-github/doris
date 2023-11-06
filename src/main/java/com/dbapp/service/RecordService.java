package com.dbapp.service;

import cn.hutool.core.io.FileUtil;
import com.dbapp.config.PTConstant;
import com.dbapp.result.PTResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.core.util.datetime.FastDateFormat;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Service;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * 记录结果
 *
 * @author yangmenglong
 * @date 2023/9/26
 */
@Service
@Slf4j
public class RecordService {

    public void record(List<PTResult> ptResults, String type, PTConstant ptConstant) {
        String fileName = "D:\\Develop\\QueryPT\\report\\" + type + "-" + ptConstant.name().toLowerCase() + "-" + FastDateFormat.getInstance("yyyyMMddHHmmss").format(System.currentTimeMillis()) + ".log";
        FileUtil.writeLines(ptResults, fileName, StandardCharsets.UTF_8);
    }

    public void recordExcel(List<PTResult> ptResults, String type, int count, PTConstant ptConstant) {
        // 创建新的Excel工作簿
        Workbook workbook = new XSSFWorkbook();
        // 创建新的工作表
        Sheet sheet = workbook.createSheet(type);

        // 表头
        List<String> headers = new ArrayList<>();
        headers.add("场景");
        for (int i = 1; i <= count; i++) {
            headers.add("第" + i + "次(耗时+时间范围)");
        }
        headers.add("平均耗时(s)");
        headers.add("失败次数");
        for (int i = 1; i <= count; i++) {
            headers.add("第" + i + "次SQL");
        }
        for (int i = 1; i <= count; i++) {
            headers.add("第" + i + "次失败原因");
        }

        // 创建新的行
        Row headerRow = sheet.createRow(0);
        for (int i = 0; i < headers.size(); i++) {
            Cell cell = headerRow.createCell(i);
            cell.setCellValue(headers.get(i));
        }

        Map<String, Map<Integer, PTResult>> resultMap = new LinkedHashMap<>();
        ptResults.forEach(ptResult -> {
            String name = ptResult.getName();
            if (!resultMap.containsKey(name)) {
                resultMap.put(name, new LinkedHashMap<>());
            }
            Map<Integer, PTResult> ptResultMap = resultMap.get(name);
            ptResultMap.put(ptResult.getCount(), ptResult);
        });

        FastDateFormat format = FastDateFormat.getInstance("yyyyMMddHHmmss");
        int rowIndex = 1;
        for (Map.Entry<String, Map<Integer, PTResult>> entry : resultMap.entrySet()) {
            int cellIndex = 0;
            Row row = sheet.createRow(rowIndex++);
            Cell cell = row.createCell(cellIndex++);
            cell.setCellValue(entry.getKey());

            double totalCost = 0;
            int failCount = 0;
            Map<Integer, PTResult> countValueMap = entry.getValue();
            for (int i = 1; i <= count; i++) {
                PTResult ptResult = countValueMap.get(i);
                Cell countCell = row.createCell(cellIndex++);
                totalCost += ptResult.getCost();
                countCell.setCellValue(ptResult.getCost()
                        + "(" + format.format(ptResult.getStart()) + "-" + format.format(ptResult.getEnd()) + ")");
                if (!ptResult.isSuccess()) {
                    failCount++;
                }
            }

            Cell avgCell = row.createCell(cellIndex++);
            double avg = Math.max(0.1, totalCost / count / 1000);
            avgCell.setCellValue(Double.parseDouble(String.format("%.1f", avg)));

            Cell failCell = row.createCell(cellIndex++);
            failCell.setCellValue(failCount);

            for (int i = 1; i <= count; i++) {
                PTResult ptResult = countValueMap.get(i);
                Cell sqlCell = row.createCell(cellIndex++);
                sqlCell.setCellValue(ptResult.getQuery());
            }

            for (int i = 1; i <= count; i++) {
                PTResult ptResult = countValueMap.get(i);
                Cell failMsgCell = row.createCell(cellIndex++);
                failMsgCell.setCellValue(ptResult.getErrorMsg());
            }

        }

        String fileName = "D:\\Develop\\QueryPT\\report\\" + type + "-" + ptConstant.name().toLowerCase() + "-" + FastDateFormat.getInstance("yyyyMMddHHmmss").format(System.currentTimeMillis()) + ".xlsx";
        try {
            FileOutputStream outputStream = new FileOutputStream(fileName);
            workbook.write(outputStream);
            workbook.close();
            outputStream.close();

            log.info("导出报告成功：{}", fileName);
        } catch (IOException e) {
            log.error("导出Excel失败", e);
        }
    }
}
