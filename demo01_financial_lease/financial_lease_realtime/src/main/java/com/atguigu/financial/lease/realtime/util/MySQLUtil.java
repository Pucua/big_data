package com.atguigu.financial.lease.realtime.util;

import com.atguigu.financial.lease.realtime.common.FinancialLeaseCommon;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class MySQLUtil {
    public static Connection getConnection(){
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(FinancialLeaseCommon.MYSQL_URL, FinancialLeaseCommon.MYSQL_USERNAME, FinancialLeaseCommon.MYSQL_PASSWD);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return connection;
    }
}
