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
import com.mvdb.etl.dao.OrderLineItemDAO;
import com.mvdb.etl.model.OrderLineItem;
import com.mvdb.etl.util.db.SequenceNames;

public class JdbcOrderLineItemDAO extends JdbcAbstractDAO<OrderLineItem>  implements OrderLineItemDAO
{

    private static final String TABLE_NAME = "order_line_item";
    private static final String PRIMARY_KEY_NAME = "order_line_item_id";
    private static final String SECONDARY_KEY_NAME = "order_id";
    
    @Override
    public OrderLineItem createRecord(ResultSet rs) throws SQLException
    {
        OrderLineItem orderLineItem = new OrderLineItem();
        orderLineItem.setOrderLineItemId(rs.getLong("ORDER_LINE_ITEM_ID"));
        orderLineItem.setOrderId(rs.getLong("ORDER_ID"));
        orderLineItem.setDescription(rs.getString("DESCRIPTION"));
        orderLineItem.setPrice(rs.getInt("PRICE"));
        orderLineItem.setPrice(rs.getInt("QUANTITY"));
        orderLineItem.setCreateTime(new java.util.Date(rs.getDate("CREATE_TIME").getTime()));
        orderLineItem.setUpdateTime(new java.util.Date(rs.getDate("UPDATE_TIME").getTime()));
        
        return orderLineItem;
    }

    @Override
    public Map<String, ColumnMetadata> findMetadata()
    {
        return super.findMetadata(TABLE_NAME);
    }

    @Override
    public void insert(OrderLineItem orderLineItem)
    {
        String sql = "INSERT INTO ORDER_LINE_ITEM "
                + "(ORDER_LINE_ITEM_ID, ORDER_ID, DESCRIPTION, PRICE, QUANTITY, CREATE_TIME, UPDATE_TIME) VALUES (?, ?, ?, ?, ?, ?, ?)";

        Object[] parameters = 
                new Object[] { orderLineItem.getOrderLineItemId(), orderLineItem.getOrderId(), 
                            orderLineItem.getDescription(), orderLineItem.getPrice(), orderLineItem.getQuantity(),
                            new java.sql.Timestamp(orderLineItem.getCreateTime().getTime()),
                            new java.sql.Timestamp(orderLineItem.getUpdateTime().getTime()) };
        
        super.insert(sql, parameters);
    }

    @Override
    public void insertBatch(final List<OrderLineItem> orderLineItems)
    {
        String sql = "INSERT INTO ORDER_LINE_ITEM "
                + "(ORDER_LINE_ITEM_ID, ORDER_ID, DESCRIPTION, PRICE, QUANTITY, CREATE_TIME, UPDATE_TIME) VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        BatchPreparedStatementSetter batchPreparedStatementSetter = new BatchPreparedStatementSetter() {

            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException
            {
                OrderLineItem orderLineItem = orderLineItems.get(i);
                ps.setLong(1, orderLineItem.getOrderLineItemId());
                ps.setLong(2, orderLineItem.getOrderId());
                ps.setString(3, orderLineItem.getDescription());
                ps.setDouble(4, orderLineItem.getPrice());
                ps.setInt(5, orderLineItem.getQuantity());
                ps.setTimestamp(6, new java.sql.Timestamp(orderLineItem.getCreateTime().getTime()));
                ps.setTimestamp(7, new java.sql.Timestamp(orderLineItem.getUpdateTime().getTime()));
            }

            @Override
            public int getBatchSize()
            {
                return orderLineItems.size();
            }
        };
        
        super.insertBatch(sql, batchPreparedStatementSetter, orderLineItems);
        
    }

    @Override
    public OrderLineItem findById(long orderLineItemId)
    {
        return super.findById(orderLineItemId, TABLE_NAME);
    }

    @Override
    public List<OrderLineItem> findAll()
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
        return super.getNextSequenceValue(SequenceNames.ORDER_LINE_ITEM_SEQUENCE_NAME);
    }

    @Override
    public void update(OrderLineItem orderLineItem)
    {
        String updateSql = "update order_line_item set order_line_item = ?, description = ?, price = ?, quantity = ?, update_time = ? where order_line_item_id = ?";
        Object[] parameters = 
                new Object[] {  orderLineItem.getOrderLineItemId(), orderLineItem.getDescription(), 
                                orderLineItem.getPrice(), orderLineItem.getQuantity(), 
                                new java.sql.Timestamp(orderLineItem.getUpdateTime().getTime()) , 
                                orderLineItem.getOrderLineItemId() };
        super.update(updateSql, parameters);
        
    }

    @Override
    public List<Long> findAllIds()
    {      
        return super.findAllIds(TABLE_NAME, PRIMARY_KEY_NAME);
    }

    @Override
    public void deleteById(long orderLineItemId)
    {
        super.deleteById(TABLE_NAME, PRIMARY_KEY_NAME, orderLineItemId);
        
    }

}
