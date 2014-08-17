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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import com.mvdb.data.ColumnMetadata;
import com.mvdb.etl.consumer.Consumer;
import com.mvdb.etl.util.db.SequenceNames;

public interface IDAO<T>
{
    public Map<String, ColumnMetadata> findMetadata(String tableName); 
    
    public void insert(String sql, Object[] parameters);

    public void insertBatch(String sql, BatchPreparedStatementSetter batchPreparedStatementSetter, final List<T> recordList);

    public T findById(final long id, String tableName);
    public T createRecord(ResultSet rs) throws SQLException;
    
    public List<T> findAll(String tableName);
    
    public void findAll(Timestamp modifiedAfter, Consumer consumer);
    
    public int findTotalRecords(String tableName);
    
    public long findMaxId(String tableName);

    public long getNextSequenceValue(SequenceNames sequenceName);

    public void executeSQl(String[] sqlList);

    public void update(String updateSql, Object[] parameters);

    public List<Long> findAllIds(String tableName, String primaryKeyName);

    public void deleteById(String tableName, String primaryKeyName, long id);

}
