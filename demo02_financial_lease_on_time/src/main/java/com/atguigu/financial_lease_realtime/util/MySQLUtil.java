package com.atguigu.financial_lease_realtime.util;

import com.atguigu.financial_lease_realtime.common.FinancialLeaseCommon;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MySQLUtil {
    public static Connection getConnection(){
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(FinancialLeaseCommon.MYSQL_URL,
                    FinancialLeaseCommon.MYSQL_USERNAME, FinancialLeaseCommon.MYSQL_PASSWD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
