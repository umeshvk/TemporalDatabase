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
package com.mvdb.etl.dao.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import com.mvdb.data.ColumnMetadata;
import com.mvdb.data.DataHeader;
import com.mvdb.data.GenericDataRecord;
import com.mvdb.data.GenericIdRecord;
import com.mvdb.data.IdRecord;
import com.mvdb.data.Metadata;
import com.mvdb.etl.consumer.GenericConsumer;
import com.mvdb.etl.consumer.SequenceFileConsumer;
import com.mvdb.etl.dao.GenericDAO;


public class JdbcGenericDAO extends JdbcDaoSupport implements GenericDAO
{
    private static Logger logger = LoggerFactory.getLogger(JdbcGenericDAO.class);
    private GlobalMvdbKeyMaker globalMvdbKeyMaker = new GlobalMvdbKeyMaker();
    
    private boolean writeDataHeader(DataHeader dataHeader, String objectName, String fileObjectName, File snapshotDirectory)
    {
        try
        {
            snapshotDirectory.mkdirs();
            String headerFileName = "header-" + fileObjectName + ".dat";
            File headerFile = new File(snapshotDirectory, headerFileName);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(dataHeader);
            FileUtils.writeByteArrayToFile(headerFile, baos.toByteArray());
            return true;
        } catch (Throwable t)
        {
            t.printStackTrace();
            return false;
        }

    }

    @Override
    public void testMetaData(String objectName)
    {
        final Metadata metadata = new Metadata();
        metadata.setTableName(objectName);
        
        String sql = "SELECT a.attname, t.typname" + 
        " FROM pg_class c, pg_attribute a, pg_type t" +  
        " WHERE c.relname = '" + objectName +  "'" + 
        " AND a.attnum >= 0" + 
        " AND a.attrelid = c.oid" + 
        " AND a.atttypid = t.oid"; 
        //String sql1 = "describe " + objectName;
        final Map<String, Object> metaDataMap = new HashMap<String, Object>();
        metadata.setColumnMetadataMap(metaDataMap);
        metadata.setTableName(objectName);

        getJdbcTemplate().query(sql, new RowCallbackHandler() {

            @Override
            public void processRow(ResultSet row) throws SQLException
            {

                    ColumnMetadata columnMetadata = new ColumnMetadata();
                    System.out.println("name:" + row.getString(1));
                    columnMetadata.setColumnLabel(row.getString(1));
                    columnMetadata.setColumnName(row.getString(1));
                    //columnMetadata.setColumnType(row.getString(1));
                    columnMetadata.setColumnTypeName(row.getString(2)); 
                    
                    metaDataMap.put(columnMetadata.getColumnName(), columnMetadata);
//                    while(row.next())
//                    {
//                }
                

            }
        });
        
        
    }
    
    @Override
    public void fetchMetadata(String objectName, File snapshotDirectory)
    {
        final Metadata metadata = new Metadata();
        final String snapShotDirName = snapshotDirectory.getName();
        metadata.setRefreshTimeStamp(snapShotDirName);
        metadata.setTableName(objectName);
        String sql = "SELECT * FROM " + objectName + " limit 1";
        final Map<String, Object> metaDataMap = new HashMap<String, Object>();
        metadata.setColumnMetadataMap(metaDataMap);
        metadata.setTableName(objectName);
        final List<Object> cmdList = new ArrayList<Object>();
        metaDataMap.put(Metadata.COLUMNDATALISTKEY, cmdList);

        getJdbcTemplate().query(sql, new RowCallbackHandler() {

            @Override
            public void processRow(ResultSet row) throws SQLException
            {
                ResultSetMetaData rsm = row.getMetaData();
                int columnCount = rsm.getColumnCount();
                for (int column = 1; column < (columnCount + 1); column++)
                {
                    ColumnMetadata columnMetadata = new ColumnMetadata();
                    columnMetadata.setColumnLabel(rsm.getColumnLabel(column));
                    columnMetadata.setColumnName(rsm.getColumnName(column));
                    columnMetadata.setColumnType(rsm.getColumnType(column));
                    columnMetadata.setColumnTypeName(rsm.getColumnTypeName(column));
                    columnMetadata.setPrecision(rsm.getPrecision(column));
                    columnMetadata.setScale(rsm.getScale(column));
                    cmdList.add(columnMetadata);
                    //metaDataMap.put(rsm.getColumnName(column), columnMetadata);
                }

            }
        });

        writeMetadata(metadata, snapshotDirectory);
    }

    private boolean writeMetadata(Metadata metadata, File snapshotDirectory)
    {
        try
        {
            File objectFile = new File(snapshotDirectory, "schema-" + metadata.getTableName().replaceAll("_", "") + ".dat");
            final GenericConsumer genericConsumer = new SequenceFileConsumer(objectFile);            
            snapshotDirectory.mkdirs();
            genericConsumer.consume(metadata);
            genericConsumer.flushAndClose();    

            return true;
        } catch (Throwable t)
        {
            t.printStackTrace();
            return false;
        }

    }
    
    @Override
    public Metadata readMetadata(String schemaFileUrl, Configuration conf)
    {
        Path path = new Path(schemaFileUrl);       
        return readMetadata(path,  conf); 
    }
    

    
    @Override
    public Metadata readMetadata(Path schemaFilePath, Configuration conf)
    {
        FileSystem fs;
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
                metadata = (Metadata) ois.readObject();
                System.out.println(metadata.toString());
            }
            System.out.println("Last Metadata:" + metadata.toString());

            
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


    @Override
    public DataHeader fetchAll2(File snapshotDirectory, Timestamp modifiedAfter, String objectName, String keyName, String updateTimeColumnName)
    {
        DataHeader dataHeader = new DataHeader();
        String fileObjectName = objectName.replaceAll("_", "");
        writeUpdates(snapshotDirectory, modifiedAfter, objectName, fileObjectName, keyName, updateTimeColumnName, dataHeader);                
        writeDataHeader(dataHeader, objectName, fileObjectName, snapshotDirectory);
        writeIds(snapshotDirectory, objectName, fileObjectName, keyName, updateTimeColumnName/*, dataHeader*/);
        return dataHeader;
    }


    
    private void writeIds(File snapshotDirectory, String objectName, String fileObjectName, final String keyName,
            final String updateTimeColumnName/*, DataHeader dataHeader*/)
    {
        final String snapShotDirName = snapshotDirectory.getName();
        //final Date refreshTimeStamp = ActionUtils.getDate(snapShotDirName);
        File objectFile = new File(snapshotDirectory, "ids-" + fileObjectName + ".dat");
        final GenericConsumer genericConsumer = new SequenceFileConsumer(objectFile);
        

        String sql = "SELECT " + keyName + " FROM " + objectName;

        getJdbcTemplate().query(sql, new Object[] {  }, new RowCallbackHandler() {

            @Override
            public void processRow(ResultSet row) throws SQLException
            {
//                final Map<String, Object> dataMap = new HashMap<String, Object>();
//                ResultSetMetaData rsm = row.getMetaData();               
                Object originalKeyValue = row.getObject(1);
                IdRecord idRecord = new GenericIdRecord(originalKeyValue , globalMvdbKeyMaker, snapShotDirName);
                genericConsumer.consume(idRecord);
                //dataHeader.incrementCount();

            }
        });

        genericConsumer.flushAndClose();
        
    }

    @Override
    public List<Object[]> getTableInfo2(String query)
    {
        final List<Object[]> result = new ArrayList<Object[]>();
        getJdbcTemplate().query(query, new Object[] { }, new RowCallbackHandler() {

            @Override
            public void processRow(ResultSet row) throws SQLException
            {
                
                ResultSetMetaData rsm = row.getMetaData();
                int columnCount = rsm.getColumnCount();
                Object[] data = new Object[columnCount];
                for (int column = 1; column <= columnCount; column++)
                {
                    data[column-1] = row.getObject(column);
                }
                result.add(data);

            }
        });
        return result;
    }
    
    public List<String[]> getTableInfo(String query)
    {
        final List<String[]> result = new ArrayList<String[]>();


        if(true) return result;
        getJdbcTemplate().query(query, new Object[] { }, new RowCallbackHandler() {

            @Override
            public void processRow(ResultSet row) throws SQLException
            {
                
                ResultSetMetaData rsm = row.getMetaData();
                int columnCount = rsm.getColumnCount();
                String[] data = new String[columnCount];
                for (int column = 1; column < (columnCount + 1); column++)
                {
                    data[column-1] = row.getString(column);
                }
                result.add(data);

            }
        });
        
        return result;
    }
    
    private void writeUpdates(File snapshotDirectory, Timestamp modifiedAfter, String objectName, String fileObjectName, final String keyName, final String updateTimeColumnName, final DataHeader dataHeader)
    {
        File objectFile = new File(snapshotDirectory, "data-" + fileObjectName + ".dat");
        final GenericConsumer genericConsumer = new SequenceFileConsumer(objectFile);
        
        final String snapShotDirName = snapshotDirectory.getName();
        //final Date refreshTimeStamp = ActionUtils.getDate(snapShotDirName);

        String sql = "SELECT * FROM " + objectName + " o where o.update_time >= ?";

        getJdbcTemplate().query(sql, new Object[] { modifiedAfter }, new RowCallbackHandler() {

            @Override
            public void processRow(ResultSet row) throws SQLException
            {
                final Map<String, Object> dataMap = new HashMap<String, Object>();
                ResultSetMetaData rsm = row.getMetaData();
                int columnCount = rsm.getColumnCount();
                for (int column = 1; column < (columnCount + 1); column++)
                {
                    dataMap.put(rsm.getColumnName(column), row.getObject(rsm.getColumnLabel(column)));
                }

                GenericDataRecord dataRecord = new GenericDataRecord(dataMap, keyName , globalMvdbKeyMaker, 
                                updateTimeColumnName, new GlobalMvdbUpdateTimeMaker());
                dataRecord.setRefreshTimeStamp(snapShotDirName);
                genericConsumer.consume(dataRecord);
                dataHeader.incrementCount();

            }
        });

        genericConsumer.flushAndClose();
    }

    @Override
    public boolean scan2(String objectName, File snapshotDirectory)
    {
        String hadoopLocalFS = "file:///";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hadoopLocalFS);
        String dataFileName = "data-" + objectName + ".dat";
        File dataFile = new File(snapshotDirectory, dataFileName);
        Path path = new Path(dataFile.getAbsolutePath());

        FileSystem fs;
        try
        {
            fs = FileSystem.get(conf);
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

            Text key = new Text(); 
            BytesWritable value = new BytesWritable(); 
            while (reader.next(key, value))
            {
                byte[] bytes = value.getBytes();
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                GenericDataRecord dr = (GenericDataRecord) ois.readObject();
                System.out.println(dr.toString());
            }

            IOUtils.closeStream(reader);
        } catch (IOException e)
        {
            logger.error("scan2():", e);
            return false;
        } catch (ClassNotFoundException e)
        {
            logger.error("scan2():", e);
            return false;
        }

        return true;
    }



    private Metadata readMetadata(String objectName, File snapshotDirectory) throws IOException, ClassNotFoundException
    {
        Metadata metadata = new Metadata();
        FileInputStream fis = null;
        ObjectInputStream ois = null;
        try
        {
            String structFileName = "schema-" + objectName + ".dat";
            File structFile = new File(snapshotDirectory, structFileName);
            fis = new FileInputStream(structFile);
            ois = new ObjectInputStream(fis);
            metadata = (Metadata) ois.readObject();
            return metadata;
        } finally
        {
            if (fis != null)
            {
                fis.close();
            }
            if (ois != null)
            {
                ois.close();
            }
        }

    }

    @Override
    public Metadata getMetadata(String objectName, File snapshotDirectory)
    {

        try
        {
            return readMetadata(objectName, snapshotDirectory);
        } catch (ClassNotFoundException e)
        {
            e.printStackTrace();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        return null;
    }




    

}
