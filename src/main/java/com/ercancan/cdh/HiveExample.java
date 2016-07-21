package com.ercancan.cdh;

import java.sql.*;

public class HiveExample {

    private static final String SQL_STATEMENT = "select * from bank_list limit 10";

    private static final String HIVE_HOST = "host_ip";
    private static final String HIVE_DB = "test_db";
    private static final String HIVE_JDBC_PORT = "10000";
    private static final String CONNECTION_URL = "jdbc:hive2://" + HIVE_HOST + ':' + HIVE_JDBC_PORT + "/"+ HIVE_DB;
    private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static final String HIVE_USERNAME = "hadoop";
    private static final String HIVE_PASSWORD = "";

    public static void main(String[] args) {

        System.out.println("\n==============HADOOP HIVE JDBC EXAMPLE==========================");
        Connection con = null;

        try {
            Class.forName(JDBC_DRIVER_NAME);
            con = DriverManager.getConnection(CONNECTION_URL, HIVE_USERNAME, HIVE_PASSWORD);

            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(SQL_STATEMENT);

            System.out.println("\n============== TEST TABLE RESULTS ==============================");
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                con.close();
            } catch (Exception e) {
            }
        }
    }
}
