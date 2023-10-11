package com.dbapp.result;

import lombok.Data;
import org.apache.logging.log4j.core.util.datetime.FastDateFormat;

/**
 * 性能测试结果
 *
 * @author yangmenglong
 * @date 2023/9/26
 */
@Data
public class PTResult {
    private String name;
    private String query;
    private long start;
    private long end;
    private long cost;
    private boolean success;
    private String errorMsg;
    private int count;

    public PTResult(String name, String query, long start, int count) {
        this.name = name;
        this.query = query;
        this.start = start;
        this.count = count;
    }

    @Override
    public String toString() {
        FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
        StringBuilder builder = new StringBuilder();
        builder.append("第").append(count).append("次：")
                .append("场景：").append(name).append(", ")
                .append("开始时间：").append(format.format(start)).append(", ")
                .append("结束时间：").append(format.format(end)).append(", ")
                .append("耗时：").append(format.format(start)).append("ms, ")
                .append("结果：").append(success ? "成功" : "失败").append(", ")
                .append("SQL：").append(query);
        if (!success) {
            builder.append(", ").append("失败原因：").append(errorMsg);
        }
        return builder.toString();
    }
}
