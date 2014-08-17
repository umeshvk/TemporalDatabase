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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import com.mvdb.data.ColumnMetadata;
import com.mvdb.etl.consumer.Consumer;
import com.mvdb.etl.dao.IDAO;
import com.mvdb.etl.util.db.SequenceNames;

public abstract class JdbcAbstractDAO<T> extends JdbcDaoSupport implements IDAO<T>
{

    @Override
    public Map<String, ColumnMetadata> findMetadata(String tableName)
    {
        String sql = String.format("SELECT * FROM %s limit 1", tableName);
        final Map<String, ColumnMetadata> metaDataMap = new HashMap<String , ColumnMetadata>();
        
        
        getJdbcTemplate().query(sql, new RowCallbackHandler() {

            @Override
            public void processRow(ResultSet row) throws SQLException
            {
                ResultSetMetaData rsm = row.getMetaData();
                int columnCount = rsm.getColumnCount();
                for(int column=1;column<(columnCount+1);column++)
                {
                    ColumnMetadata metadata = new ColumnMetadata();
                    metadata.setColumnLabel(rsm.getColumnLabel(column));
                    metadata.setColumnName(rsm.getColumnName(column));
                    metadata.setColumnType(rsm.getColumnType(column));
                    metadata.setColumnTypeName(rsm.getColumnTypeName(column));
                    metadata.setPrecision(rsm.getPrecision(column));
                    metadata.setScale(rsm.getScale(column));                    
                    metaDataMap.put(rsm.getColumnName(column), metadata);
                }
                
            }
        });
        
        return metaDataMap; 
    }

    @Override
    public void insert(String sql, Object[] parameters)
    {      
        getJdbcTemplate().update(sql, parameters);       
    }

    @Override
    public void insertBatch(String sql, BatchPreparedStatementSetter batchPreparedStatementSetter, final List<T> recordList)
    {      
        getJdbcTemplate().batchUpdate(sql, batchPreparedStatementSetter);       
    }

    @Override
    public T findById(final long id, String tableName)
    {
        String sql = String.format("SELECT * FROM %s WHERE ORDER_ID = ?", tableName);
        T order = (T)getJdbcTemplate().query(
                 sql,
                 new PreparedStatementSetter() {
                   public void setValues(PreparedStatement preparedStatement) throws
                     SQLException {
                       preparedStatement.setLong(1, id);
                   }
                 }, 
                 new ResultSetExtractor() {
                   public T extractData(ResultSet resultSet) throws SQLException,
                     DataAccessException {
                       if (resultSet.next()) {
                           return createRecord(resultSet);
                       }
                       return null;
                   }
                 }
             );

        return order;
    }
    
    public abstract T createRecord(ResultSet rs) throws SQLException;
    /**
     return com.mvdb.etl.model.OrderRowMapper.createOrder(resultSet);
     */

    @Override
    public List<T> findAll(String tableName)
    {
        String sql = String.format("SELECT * FROM %s", tableName);

        List<T> orders = findAll(sql);

        return orders;
    }

    @Override
    public void findAll(Timestamp modifiedAfter, final Consumer consumer)
    {
        String sql = "SELECT * FROM ORDERS where orders.update_time >= ?";

        
        getJdbcTemplate().query(sql, new Object[] { modifiedAfter }, new RowCallbackHandler() {

            @Override
            public void processRow(ResultSet row) throws SQLException
            {
                
                T record = createRecord(row);

                consumer.consume(record);
            }
        });
        
    }



    @Override
    public int findTotalRecords(String tableName)
    {
        String sql = String.format("SELECT COUNT(*) FROM %s", tableName);

        int total = getJdbcTemplate().queryForInt(sql);

        return total;
    }

    @Override
    public long findMaxId(String tableName)
    {
        String sql = String.format("SELECT MAX(Order_Id) FROM %s", tableName);

        long max = getJdbcTemplate().queryForLong(sql);

        return max;
    }

    @Override
    public long getNextSequenceValue(SequenceNames sequenceName)
    {
        String sql = "SELECT nextval('" + sequenceName.getName() + "');";

        long value = getJdbcTemplate().queryForLong(sql);

        return value;
    }

    @Override
    public void executeSQl(String[] sqlList)
    {
        for (String sql : sqlList)
        {
            getJdbcTemplate().update(sql);
        }
    }

    @Override
    public void update(String updateSql, Object[] parameters)
    {
        getJdbcTemplate().update(updateSql,parameters);
    }

    @Override
    public List<Long> findAllIds(String tableName, String primaryKeyName)
    {
        String sql = String.format("SELECT %s FROM %s", primaryKeyName, tableName);
        List<Long> idList = new ArrayList<Long>();
        List<Map> rows = getJdbcTemplate().queryForList(sql);
        for (Map row : rows)
        {
            idList.add((Long) (row.get(primaryKeyName)));
        }
        return idList;
    }

    @Override
    public void deleteById(String tableName, String primaryKeyName, long id)
    {
        String sql = String.format("delete from %s where %s = ?", tableName, primaryKeyName);
        getJdbcTemplate().update(sql, new Object[] {id});        
        
    }

 




}
