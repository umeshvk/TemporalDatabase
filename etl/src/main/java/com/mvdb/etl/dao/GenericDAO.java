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
package com.mvdb.etl.dao;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.mvdb.data.DataHeader;
import com.mvdb.data.Metadata;


public interface GenericDAO
{
    void testMetaData(String objectName);
    void fetchMetadata(String objectName, File snapshotDirectory);
    //DataHeader fetchAllOld(File snapshotDirectory, Timestamp modifiedAfter, String objectName);
    //boolean scan(File file, int count);
    //boolean scanOld(String objectName, File snapshotDirectory) throws IOException;
    boolean scan2(String objectName, File snapshotDirectory) throws IOException;
    Metadata getMetadata(String objectName, File snapshotDirectory);
    //DataHeader fetchAll2(File snapshotDirectory, Timestamp modifiedAfter, String objectName, String keyName);
    DataHeader fetchAll2(File snapshotDirectory, Timestamp modifiedAfter, String objectName, String keyName,
            String updateTimeColumnName);
    List<String[]> getTableInfo(String query);
    List<Object[]> getTableInfo2(String query);
    Metadata readMetadata(String schemaFileUrl, Configuration conf);
    Metadata readMetadata(Path schemaFilePath, Configuration conf);
    /*
    Metadata readLatestMetadata(Path schemaFilePath, Configuration conf);    
    Metadata readLatestMetadata(String schemaFileUrl, Configuration conf);
    */
}