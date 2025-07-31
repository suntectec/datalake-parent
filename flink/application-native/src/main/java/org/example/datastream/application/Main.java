package org.example.datastream.application;

import org.example.util.MyParameter;

/**
 * @author Jagger
 * @since 2025/7/31 9:44
 */
public class Main {
    public static void main(String[] args) throws Exception {
        String sqlServerHost = MyParameter.getParameter("dev", "sqlserver.host");
        String sqlServerPort = MyParameter.getParameter("dev", "sqlserver.port");
        String sqlServerUsername = MyParameter.getParameter("dev", "sqlserver.username");
        String sqlServerPassword = MyParameter.getParameter("dev", "sqlserver.password");

        String mySqlServerHost = MyParameter.getParameter("dev", "my.sqlserver.host");
        String mySqlServerPort = MyParameter.getParameter("dev", "my.sqlserver.port");
        String mySqlServerUsername = MyParameter.getParameter("dev", "my.sqlserver.username");
        String mySqlServerPassword = MyParameter.getParameter("dev", "my.sqlserver.password");

        // SQLCreateSqlServer2PaimonFile.run(mySqlServerHost, mySqlServerPort, mySqlServerUsername, mySqlServerPassword);
        TableAPISqlServer2PaimonS3.run(mySqlServerHost, mySqlServerPort, mySqlServerUsername, mySqlServerPassword);
        // TableAPIPaimonS32Console.run();
    }
}
