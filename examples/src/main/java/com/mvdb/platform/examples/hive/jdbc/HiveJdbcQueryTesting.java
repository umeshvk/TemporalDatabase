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
package com.mvdb.platform.examples.hive.jdbc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mvdb.data.ColumnMetadata;
import com.mvdb.data.GenericDataRecord;
import com.mvdb.data.Metadata;
import com.mvdb.data.MultiVersionRecord;
import com.mvdb.etl.actions.ActionUtils;




public class HiveJdbcQueryTesting
{
    private static Logger logger = LoggerFactory.getLogger(HiveJdbcQueryTesting.class);
    
    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

    public static Metadata readLatestMetadata(String schemaFileUrl, Configuration conf)
    {
        Path path = new Path(schemaFileUrl);       
        return readLatestMetadata(path,  conf); 
    }
    
    public static Metadata readLatestMetadata(Path schemaFilePath, Configuration conf)
    {
        FileSystem fs;
        MultiVersionRecord mvr = null;
        Metadata metadata = null; 
        SequenceFile.Reader reader = null; 
        try
        {
            
            fs = FileSystem.get(conf);
            reader = new SequenceFile.Reader(fs, schemaFilePath, conf);

            Text key = new Text(); 
            BytesWritable value = new BytesWritable();
            
            while (reader.next(key, value))
            {
                byte[] bytes = value.getBytes();
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                mvr = (MultiVersionRecord) ois.readObject();
                metadata = (Metadata)mvr.getLatestVersion();
                //System.out.println(metadata.toString());
            }
            //System.out.println("Last Metadata:" + metadata.toString());

            
        } catch (IOException e)
        {
            logger.error("readMetadata():", e);
            return null;
        } catch (ClassNotFoundException e)
        {
            logger.error("readMetadata():", e);
            return null;
        } finally { 
            IOUtils.closeStream(reader);
        }
        return metadata; 

    }
    /**
     * @param args
     * @throws SQLException
     * @throws IOException 
     */
    public static void main(String[] args) throws SQLException, IOException
    {
        ActionUtils.setUpInitFileProperty();

        Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration.addResource(new Path("/home/umesh/ops/hadoop-1.2.0/conf/core-site.xml"));
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        //Also add  lastMergedTimeStamp and  mergeUptoTimestamp and passive db name which would be mv1 or mv2
        if (otherArgs.length != 2)
        {
            System.err.println("Usage: HiveJdbcQueryTesting <customer-directory> <tableListCSV>");
            System.exit(1);
        }
        //Example: file:/home/umesh/.mvdb/etl/data/alpha
        //Example: hdfs://localhost:9000/data/alpha
        String customerDirectory = otherArgs[0];
        String tableListCSV = otherArgs[1];
        String[] tables =  ActionUtils.getTokensFromCSV(tableListCSV, ",");
        String customerName = new Path(customerDirectory).getName();
        
        String activeDBName = ActionUtils.getActiveDBName(customerName);
        
        try
        {
            Class.forName(driverName);
        } catch (ClassNotFoundException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection("jdbc:hive://localhost:10000/" + "default", "", "");
        Statement stmt = con.createStatement();
        stmt.executeQuery("add jar /home/umesh/work/BigData/etl/etl/target/etl-0.0.1.jar");
        stmt.executeQuery("add jar /home/umesh/work/BigData/mvdb/target/mvdb-0.0.1.jar");
        stmt.executeQuery("add jar /home/umesh/ops/hive-0.11.0-bin/lib/hive-contrib-0.11.0.jar");
        
        dropTables("mv1", tables, stmt);
        createTables("mv1", customerDirectory, activeDBName, configuration, customerName, tables, stmt);
        dropTables("mv2", tables, stmt);
        createTables("mv2", customerDirectory, activeDBName, configuration, customerName, tables, stmt);
        
        showTables("mv1", stmt);
        describeTables("mv1", tables, stmt);
        showTables("mv2", stmt);
        describeTables("mv2", tables, stmt);
        
        showTableRecords("mv1", stmt);
        showTableRecords("mv2", stmt);
        
    }

    private static void showTableRecords(String databaseName, Statement stmt) throws SQLException
    {
        stmt.executeQuery("use " + databaseName);
        ResultSet res = null;
        String sql = "show tables";
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next())
        {
            String tableName = res.getString(1);
            showRecords(databaseName, tableName, null, stmt);
        }
    }
    
    //timestamp =  2003-01-19 00:00:00
    //stmt.executeQuery("set sliceDate=2003-01-19 00:00:00;");
    private static void showRecords(String databaseName, String tableName, String timestamp, Statement stmt) throws SQLException
    {
        long t1 =  new Date().getTime();
        try { 
            stmt.executeQuery("set sliceDate=" + timestamp + ";");
            System.out.println("Display Records for Table:" + tableName);
            displayRecords(tableName, stmt);
        }finally{ 
            long t2 =  new Date().getTime();
            System.out.println("Time Taken in secs:" + ((double)(t2-t1))/1000);
        }
    }

    private static void displayRecords(String tableName, Statement stmt) throws SQLException
    {
        ResultSet res = stmt.executeQuery("select * from " + tableName);
        int colCount = res.getMetaData().getColumnCount();
        while (res.next())
        {
            for(int c=1;c<=colCount;c++)
            {
                System.out.print(res.getString(c) + "\t");
            }
            System.out.println();
        }
        
    }

    // show tables
    private static void showTables(String databaseName,  Statement stmt) throws SQLException
    {
        stmt.executeQuery("use " + databaseName);
        ResultSet res = null;
        String sql = "show tables";
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next())
        {
            System.out.println(res.getString(1));
        }
    }
    
    private static void describeTables(String databaseName,  String[] tables, Statement stmt) throws SQLException
    {
        stmt.executeQuery("use " + databaseName);
        ResultSet res = null;
        for(String tableName : tables)
        {
            tableName = tableName.replace("-", "");
            tableName = tableName.replace("_", "");
            String sql = "describe " + tableName;
            res = stmt.executeQuery(sql);
            int colCount = res.getMetaData().getColumnCount();
            while (res.next())
            {
                for(int c=1;c<=colCount;c++)
                {
                    System.out.print(res.getString(c) + "\t");
                }
                System.out.println();
            }
            
        }
    }

    private static void createTables(String databaseName, String customerDirectory, String activeDBName, Configuration configuration, String customerName, String[] tables, Statement stmt) throws SQLException
    {
        List<String> createTableQueryList = new ArrayList<String>();
        for(String tableName : tables)
        {
            tableName = tableName.replace("-", "");
            tableName = tableName.replace("_", "");
            String schemaFile = customerDirectory + "/db/" + activeDBName + "/schema" + tableName + "/" + "schema" + tableName + "-r-00000";
            Path schemaFilePath = new Path(schemaFile);
            Metadata metaData = readLatestMetadata(schemaFilePath, configuration);
            Map <String, Object> schemaMap = metaData.getColumnMetadataMap();
            String tableCreationQuery = createTableCreationQuery(tableName, customerName, activeDBName,schemaMap);
            createTableQueryList.add(tableCreationQuery);
            System.out.println(tableCreationQuery);
        }
        
        stmt.executeQuery("use " + databaseName);

        
        try { 

            for(String createQuery : createTableQueryList)
            {
                stmt.executeQuery(createQuery);
            }
        } catch(Throwable t) { 
            t.printStackTrace();
            System.exit(1);
        } finally { 
            //
        }

        
    }

    private static void dropTables(String databaseName, String[] tables, Statement stmt) throws SQLException
    {
        stmt.executeQuery("use " + databaseName);
        List<String> dropTableQueryList = new ArrayList<String>();
        for(String tableName : tables)
        {
            tableName = tableName.replace("-", "");
            tableName = tableName.replace("_", "");
            String tableDropQuery = dropTableCreationQuery(tableName);
            dropTableQueryList.add(tableDropQuery);
            System.out.println(tableDropQuery);
        }
        
        
        try { 
            for(String dropQuery : dropTableQueryList)
            {
                stmt.executeQuery(dropQuery);
            }

        } catch(Throwable t) { 
            t.printStackTrace();
            System.exit(1);
        } finally { 
            //
        }
        
    }

    /**
CREATE EXTERNAL TABLE page_view_stg(viewTime INT, userid BIGINT,
                page_url STRING, referrer_url STRING,
                ip STRING COMMENT 'IP Address of the User',
                country STRING COMMENT 'country of origination')
COMMENT 'This is the staging page view table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '44' LINES TERMINATED BY '12'
STORED AS TEXTFILE
LOCATION '/user/data/staging/page_view';  
     * @param targetTableName
     * @param schemaMap
     * @return
     */
    
    private static String translateToHiveDataType(String sqlDataType, int precision, int scale)
    {

        if("int8".equalsIgnoreCase(sqlDataType))
        {
            return "BIGINT";
        }
        else if("int4".equalsIgnoreCase(sqlDataType))
        {
            return "INT";
        }
        else if("int2".equalsIgnoreCase(sqlDataType))
        {
            return "SMALLINT";
        }
        else if("int1".equalsIgnoreCase(sqlDataType))
        {
            return "TINYINT";
        }
        else if("varchar".equalsIgnoreCase(sqlDataType))
        {
            return "STRING";
        }
        else if("timestamp".equalsIgnoreCase(sqlDataType))
        {
            return "STRING";
        }
        else if("numeric".equalsIgnoreCase(sqlDataType))
        {
//            if(precision > 0)
//            {
//                if(scale < 0)
//                {
//                    scale = 0;
//                }
//                return "DECIMAL(" + precision + ", " + scale + ")";
//            }
            //DECIMAL is supported, but appears that precision and scale are not supported yet.
            if(precision > 0)
            {               
                return "DOUBLE"; //return "DECIMAL";
            }
            else
            {
                return "DOUBLE";
            }
        }
        else 
        {
            throw new RuntimeException("Unsupported data type:" + sqlDataType);
        }
        
    }
    //drop table if exists orders
    
    private static String dropTableCreationQuery(String targetTableName)
    {
        StringBuffer queryBuffer = new StringBuffer();
        queryBuffer.append("drop table if exists " + targetTableName);
        return queryBuffer.toString();
    }
    
    private static String createTableCreationQuery(String targetTableName, String customerName, String activeDBName, Map<String, Object> schemaMap)
    {
        StringBuffer queryBuffer = new StringBuffer();
        queryBuffer.append("create external table ");
        queryBuffer.append(targetTableName + "(");
        
        String csvSeparator = ", "; 
        List<Object> cmdList = (List<Object>)schemaMap.get(Metadata.COLUMNDATALISTKEY);
        boolean hasColumns = false;
        for(int i=0;i<cmdList.size();i++)
        {
            hasColumns = true;
            ColumnMetadata columnMetadata = (ColumnMetadata)cmdList.get(i);          
            String columnName = columnMetadata.getColumnName(); 
            queryBuffer.append(columnName + " ");
            String columnTypeName = columnMetadata.getColumnTypeName();
            String hiveTypeName = translateToHiveDataType(columnTypeName, columnMetadata.getPrecision(), columnMetadata.getScale());
            queryBuffer.append(hiveTypeName + csvSeparator);
        }
        queryBuffer.append(GenericDataRecord.MVDB_IS_DELETED_COLUMN + " BOOLEAN");
        //queryBuffer.setLength(queryBuffer.length()-csvSeparator.length()); 
        //  /data/alpha/db/mv2/schemaorders/schemaorders-r-00000
        String dataSource = "/data/" + customerName + "/db/" + activeDBName + "/" + targetTableName;
        queryBuffer.append(") row format serde 'com.mvdb.platform.action.TimeSliceSerde' stored as sequencefile location '" + dataSource + "'");
        return queryBuffer.toString();
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
