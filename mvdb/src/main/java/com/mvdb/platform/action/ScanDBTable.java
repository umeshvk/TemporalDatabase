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
package com.mvdb.platform.action;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mvdb.data.GenericDataRecord;
import com.mvdb.data.GenericIdRecord;
import com.mvdb.data.IdRecord;
import com.mvdb.data.MultiVersionRecord;
import com.mvdb.etl.actions.ScanDBChanges;

public class ScanDBTable
{
    private static Logger logger = LoggerFactory.getLogger(ScanDBChanges.class);
    /**
     * @param args
     */
    public static void main(String[] args)
    {
        //ScanDBTable.scan("/home/umesh/.mvdb/etl/data/alpha/20030115050607/ids-orders.dat"); 
        //ScanDBTable.scan("/home/umesh/.mvdb/etl/data/alpha/20030131050607/ids-orders.dat"); 
        ///home/umesh/.mvdb/etl/data/alpha/20030117050607
        //ScanDBTable.scan("/home/umesh/.mvdb/etl/data/alpha/db/tmp-49728/orders-r-00000"); 
        //ScanDBTable.scanHdfsFile("hdfs://localhost:9000/data/alpha/db/mv2/orders/orders-r-00000"); 
        //
        ScanDBTable.scanLocalFile("/home/umesh/.mvdb/etl/data/alpha/20030117050607/data-orderlineitem.dat"); 
    }

    public static boolean scanHdfsFile(String dataFileName)
    {

        Configuration conf = new Configuration();
        conf.addResource(new Path("/home/umesh/ops/hadoop-1.2.0/conf/core-site.xml"));
        
        Path path = new Path(dataFileName);
        
        return scan(path, conf);
    }
    
    public static boolean scanLocalFile(String dataFileName)
    {
        File dataFile = new File(dataFileName);
        Configuration conf = new Configuration();
        //conf.addResource(new Path("/home/umesh/ops/hadoop-1.2.0/conf/core-site.xml"));
        Path path = new Path("file:" + dataFile.getAbsolutePath());
        
        return scan(path, conf);
    }
    
    public static boolean scan(Path path, Configuration conf)
    {


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
                Object object = ois.readObject();
                System.out.println("object type:" + object.getClass().getCanonicalName());
                if(object instanceof GenericDataRecord)
                {
                    GenericDataRecord dr = (GenericDataRecord) object;
                    System.out.println(dr.toString());
                }
                else if(object instanceof MultiVersionRecord)
                {
                    MultiVersionRecord mvr = (MultiVersionRecord) object;
                    System.out.println(mvr.toString());
                }
                else if(object instanceof GenericIdRecord)
                {
                    IdRecord idr = (IdRecord) object;
                    System.out.println(idr.toString());
                }
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
}
