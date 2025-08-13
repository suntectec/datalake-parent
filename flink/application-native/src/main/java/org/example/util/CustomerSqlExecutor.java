package org.example.util;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink SQL 执行器
 *
 * @author Jagger
 * @since 2025/7/31 10:04
 */
public class CustomerSqlExecutor {

    // 类级别定义Logger
    private static final Logger logger = LoggerFactory.getLogger(CustomerSqlExecutor.class);

    /**
     * 执行Flink SQL并记录执行过程和结果
     *
     * @param tEnv TableEnvironment实例
     * @param sql  要执行的SQL语句
     * @return 执行是否成功
     */
    public static boolean executeSQL(TableEnvironment tEnv, String sql) {
        // System.out.println("Executing SQL: " + sql);  // 打印要执行的SQL
        logger.info("Executing SQL: {}", sql);  // 使用日志记录要执行的SQL

        try {
            // ... 业务代码 ...
            TableResult tableResult = tEnv.executeSql(sql);

            // 打印执行结果基本信息
            // System.out.println("SQL executed successfully");
            logger.info("SQL executed successfully");

            // 如果是查询语句，可以打印结果摘要
            if (tableResult.getJobClient().isPresent()) {
                tableResult.getJobClient().get().getJobStatus();
                // System.out.println("This SQL triggered a Flink job");
                logger.info("This SQL triggered a Flink job");
            } else {
                // System.out.println("This SQL did not trigger a Flink job (likely DDL)");
                logger.info("This SQL did not trigger a Flink job (likely DDL)");
            }

            // 尝试收集并打印结果（对于SELECT等查询语句）
            try {
                tableResult.print();  // 打印查询结果
            } catch (UnsupportedOperationException e) {
                // 对于不支持的打印操作（如DDL），忽略
            }

            return true;

        } catch (TableException e) {
            // System.err.println("Failed to execute SQL: " + sql);
            // System.err.println("Error: " + e.getMessage());
            // e.printStackTrace();

            // 在正式的生产代码中，直接使用 printStackTrace() 是不推荐的，因为它会将堆栈跟踪直接打印到标准错误输出，缺乏灵活性且不利于日志管理。应该使用更健壮的日志框架（如 SLF4J + Logback 或 Log4j2）
            // 替换printStackTrace()
            logger.error("Failed to execute SQL: {}", sql, e); // 自动包含堆栈跟踪
            // 或者只记录错误消息（不带堆栈）
            // logger.error("Failed to execute SQL: {}. Error: {}", sql, e.getMessage());

            return false;
        }
    }

    /**
     * 批量执行多条SQL语句
     *
     * @param tEnv TableEnvironment实例
     * @param sqls SQL语句数组
     * @return 是否全部执行成功
     */
    public static boolean executeBatch(TableEnvironment tEnv, String[] sqls) {
        boolean allSuccess = true;
        for (String sql : sqls) {
            if (!executeSQL(tEnv, sql)) {
                allSuccess = false;
            }
        }
        return allSuccess;
    }
}
