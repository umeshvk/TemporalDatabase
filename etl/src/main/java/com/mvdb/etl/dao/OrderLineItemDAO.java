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

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import com.mvdb.data.ColumnMetadata;
import com.mvdb.etl.consumer.Consumer;
import com.mvdb.etl.model.OrderLineItem;

public interface OrderLineItemDAO
{
    public Map<String, ColumnMetadata> findMetadata(); 
    
    public void insert(OrderLineItem orderLineItem);

    public void insertBatch(List<OrderLineItem> customer);

    public OrderLineItem findById(long orderLineItemId);

    public List<OrderLineItem> findAll();
    
    public void findAll(Timestamp modifiedAfter, Consumer consumer);
    
    public int findTotalRecords();
    
    public long findMaxId();

    public long getNextSequenceValue();

    public void executeSQl(String[] sqlList);

    public void update(OrderLineItem orderLineItem);

    public List<Long> findAllIds();

    public void deleteById(long orderLineItemId);

}
