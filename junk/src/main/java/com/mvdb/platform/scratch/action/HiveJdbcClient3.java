/*******************************************************************************
 * Copyright 2014 Umesh Kanitkar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.mvdb.platform.scratch.action;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

public class HiveJdbcClient3
{
    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

    /**
     * @param args
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException
    {
        try
        {
            Class.forName(driverName);
        } catch (ClassNotFoundException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection("jdbc:hive://localhost:10000/mv1", "", "");
        Statement stmt = con.createStatement();
        String tableName = "orders";
        ResultSet res = null;
        // show tables
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        if (res.next())
        {
            System.out.println(res.getString(1));
        }
        // describe table
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next())
        {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }

        
        stmt.executeQuery("add jar /home/umesh/work/BigData/etl/etl/target/etl-0.0.1.jar");
        stmt.executeQuery("add jar /home/umesh/work/BigData/mvdb/target/mvdb-0.0.1.jar");
        stmt.executeQuery("add jar /home/umesh/ops/hive-0.11.0-bin/lib/hive-contrib-0.11.0.jar");
        long t1 =  new Date().getTime();

        
        try { 
            stmt.executeQuery("set sliceDate=2003-01-19 00:00:00;");
            for(int i=0;i<2;i++)
            {
                testSelect("0000000000000007", stmt);
            }
        }finally{ 
            long t2 =  new Date().getTime();
            System.out.println("Time Taken in secs:" + ((double)(t2-t1))/1000);
        }


        /*
         * // load data into table // NOTE: filepath has to be local to the hive
         * server // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields
         * per line
         * 
         * 
         * String filepath = "/tmp/a.txt"; sql = "load data local inpath '" +
         * filepath + "' into table " + tableName;
         * System.out.println("Running: " + sql); res = stmt.executeQuery(sql);
         * 
         * // select * query sql = "select * from " + tableName;
         * System.out.println("Running: " + sql); res = stmt.executeQuery(sql);
         * while (res.next()) { System.out.println(String.valueOf(res.getInt(1))
         * + "\t" + res.getString(2)); }
         * 
         * // regular hive query sql = "select count(1) from " + tableName;
         * System.out.println("Running: " + sql); res = stmt.executeQuery(sql);
         * while (res.next()) { System.out.println(res.getString(1)); }
         */
    }

    private static void testSelect(String id, Statement stmt) throws SQLException
    {
        // select * query
        String sql = "select * from test_table where mvdb_id ='" + id + "'";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
//        ResultSetMetaData rsmd = res.getMetaData();
//        int colCount = rsmd.getColumnCount(); 
//        String columnName = rsmd.getColumnName(1);
        while (res.next())
        {            
            System.out.println(res.getString(1) + "\t" + res.getString(2) + "\t" + res.getString(3) + "\t" + res.getString(4) + "\t" + res.getString(5));
        }

    }
}
