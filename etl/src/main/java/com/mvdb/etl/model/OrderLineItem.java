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
package com.mvdb.etl.model;

import java.io.Serializable;
import java.util.Date;

public class OrderLineItem implements Serializable
{
    long   orderLineItemId;
    long   orderId;
    String description;
    int    quantity;
    double price; 
    Date   createTime;
    Date   updateTime;

    public OrderLineItem()
    {
    }

    public OrderLineItem(long   orderLineItemId, long orderId, String description, int quantity, double price, Date createTime, Date updateTime)
    {
        this.orderLineItemId = orderLineItemId;
        this.orderId = orderId;
        this.description = description; 
        this.quantity = quantity;
        this.price = price; 
        this.createTime = createTime;
        this.updateTime = updateTime;
    }

    public long getOrderLineItemId()
    {
        return orderLineItemId;
    }

    public void setOrderLineItemId(long orderLineItemId)
    {
        this.orderLineItemId = orderLineItemId;
    }

    public long getOrderId()
    {
        return orderId;
    }

    public void setOrderId(long orderId)
    {
        this.orderId = orderId;
    }

    public String getDescription()
    {
        return description;
    }

    public void setDescription(String description)
    {
        this.description = description;
    }

    public int getQuantity()
    {
        return quantity;
    }

    public void setQuantity(int quantity)
    {
        this.quantity = quantity;
    }

    public double getPrice()
    {
        return price;
    }

    public void setPrice(double price)
    {
        this.price = price;
    }

    public Date getCreateTime()
    {
        return createTime;
    }

    public void setCreateTime(Date createTime)
    {
        this.createTime = createTime;
    }

    public Date getUpdateTime()
    {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime)
    {
        this.updateTime = updateTime;
    }

    @Override
    public String toString()
    {
        return "OrderLineItem [orderLineItemId=" + orderLineItemId + ", orderId=" + orderId + 
                    ", description=" + description +  ", quantity=" + quantity +   
                    ", price=" + price + ", createTime=" + createTime + ", updateTime=" + updateTime + "]";
    }

}
