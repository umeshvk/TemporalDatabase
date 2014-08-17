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
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import com.mvdb.data.ColumnMetadata;
import com.mvdb.etl.dao.OrderDAO;
import com.mvdb.etl.model.Order;
import com.mvdb.etl.util.db.SequenceNames;

public class JdbcOrderDAO extends JdbcAbstractDAO<Order>  implements OrderDAO
{

    private static final String TABLE_NAME = "orders";
    private static final String PRIMARY_KEY_NAME = "order_id";
    
    @Override
    public Order createRecord(ResultSet rs) throws SQLException
    {
        Order order = new Order();
        order.setOrderId(rs.getLong("ORDER_ID"));
        order.setNote(rs.getString("NOTE"));
        order.setSaleCode(rs.getInt("SALE_CODE"));
        order.setCreateTime(new java.util.Date(rs.getDate("CREATE_TIME").getTime()));
        order.setUpdateTime(new java.util.Date(rs.getDate("UPDATE_TIME").getTime()));
        
        return order;
    }

    @Override
    public Map<String, ColumnMetadata> findMetadata()
    {
        return super.findMetadata(TABLE_NAME);
    }

    @Override
    public void insert(Order order)
    {
        String sql = "INSERT INTO ORDERS "
                + "(ORDER_ID, NOTE, SALE_CODE, CREATE_TIME, UPDATE_TIME) VALUES (?, ?, ?, ?, ?)";

        Object[] parameters = 
                new Object[] { order.getOrderId(), order.getNote(), order.getSaleCode(),
                        new java.sql.Timestamp(order.getCreateTime().getTime()),
                        new java.sql.Timestamp(order.getUpdateTime().getTime()) };
        
        super.insert(sql, parameters);
    }

    @Override
    public void insertBatch(final List<Order> orders)
    {
        String sql = "INSERT INTO ORDERS "
                + "(ORDER_ID, NOTE, SALE_CODE, CREATE_TIME, UPDATE_TIME) VALUES (?, ?, ?, ?, ?)";
        
        BatchPreparedStatementSetter batchPreparedStatementSetter = new BatchPreparedStatementSetter() {

            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException
            {
                Order order = orders.get(i);
                ps.setLong(1, order.getOrderId());
                ps.setString(2, order.getNote());
                ps.setInt(3, order.getSaleCode());
                ps.setTimestamp(4, new java.sql.Timestamp(order.getCreateTime().getTime()));
                ps.setTimestamp(5, new java.sql.Timestamp(order.getUpdateTime().getTime()));
            }

            @Override
            public int getBatchSize()
            {
                return orders.size();
            }
        };
        
        super.insertBatch(sql, batchPreparedStatementSetter, orders);
        
    }

    @Override
    public Order findById(long orderId)
    {
        return super.findById(orderId, TABLE_NAME);
    }

    @Override
    public List<Order> findAll()
    {
        return super.findAll(TABLE_NAME);
    }

    @Override
    public int findTotalRecords()
    {        
        return  super.findTotalRecords(TABLE_NAME);
    }

    @Override
    public long findMaxId()
    {
        return super.findMaxId(TABLE_NAME);
    }

    @Override
    public long getNextSequenceValue()
    {
        return super.getNextSequenceValue(SequenceNames.ORDER_SEQUENCE_NAME);
    }

    @Override
    public void update(Order order)
    {
        String updateSql = "update orders set note = ?, sale_code = ?, update_time = ? where order_id = ?";
        Object[] parameters = 
                new Object[] { order.getNote(), order.getSaleCode(), 
                                new java.sql.Timestamp(order.getUpdateTime().getTime()) , 
                                order.getOrderId() };
        super.update(updateSql, parameters);
        
    }

    @Override
    public List<Long> findAllIds()
    {      
        return super.findAllIds(TABLE_NAME, PRIMARY_KEY_NAME);
    }

    @Override
    public void deleteById(long orderId)
    {
        super.deleteById(TABLE_NAME, PRIMARY_KEY_NAME, orderId);
        
    }

}
