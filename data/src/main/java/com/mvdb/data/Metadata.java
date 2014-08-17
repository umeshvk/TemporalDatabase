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
package com.mvdb.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Metadata implements AnyRecord, Externalizable, Comparable<Metadata>
{
    private static final long  serialVersionUID  = 1L;
    public static final String COLUMNDATALISTKEY = "columnDataListKey";

    String                     refreshTimeStamp;
    int                        count;
    String                     schemaName;
    String                     tableName;
    // ColumnMetadata
    Map<String, Object>        columnMetadataMap = new HashMap<String, Object>();

    public static void main1(String[] args) throws IOException, ClassNotFoundException
    {

        Metadata m1 = new Metadata();
        m1.setCount(1);
        m1.setRefreshTimeStamp("20120101000000");
        m1.setSchemaName("schema");
        m1.setTableName("table");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream so = new ObjectOutputStream(baos);
        so.writeObject(m1);
        so.flush();
        
        byte[] theData = baos.toByteArray();
        
        ByteArrayInputStream bais = new ByteArrayInputStream(theData);
        ObjectInputStream ois = new ObjectInputStream(bais);
        Metadata m2 = (Metadata)ois.readObject();
        
        System.out.println(m1);
        System.out.println(m2);
      
        
    }
    
    public Metadata()
    {
        count = 0;
    }

    public String getRefreshTimeStamp()
    {
        return refreshTimeStamp;
    }

    public void setRefreshTimeStamp(String refreshTimeStamp)
    {
        this.refreshTimeStamp = refreshTimeStamp;
    }

    public void incrementCount()
    {
        count++;
    }

    public int getCount()
    {
        return count;
    }

    public void setCount(int count)
    {
        this.count = count;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public Map<String, Object> getColumnMetadataMap()
    {
        return columnMetadataMap;
    }

    public void setColumnMetadataMap(Map<String, Object> columnMetadataMap)
    {
        this.columnMetadataMap = columnMetadataMap;
    }

    @Override
    public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException
    {
        count = input.readInt();
        schemaName = (String) input.readObject();
        tableName = (String) input.readObject();
        refreshTimeStamp = (String) input.readObject();

        columnMetadataMap = new HashMap<String, Object>();
        int keyCount = input.readInt();
        List<Object> columnDataObjectList = new ArrayList<Object>();
        columnMetadataMap.put(COLUMNDATALISTKEY, columnDataObjectList);
        if(keyCount > -1)
        {
            for (int i = 0; i < keyCount; i++)
            {
                // String key = (String)input.readObject();
                // ColumnMetadata columnMetadata = new ColumnMetadata();
                // columnMetadata = (ColumnMetadata) input.readObject();
                // columnMetadataMap.put(key, columnMetadata);
                ColumnMetadata columnMetadata = (ColumnMetadata) input.readObject();
                columnDataObjectList.add(columnMetadata);
            }
        }

    }

    @Override
    public void writeExternal(ObjectOutput output) throws IOException
    {
        output.writeInt(count);
        output.writeObject(schemaName);
        output.writeObject(tableName);
        output.writeObject(refreshTimeStamp);

        List<Object> columnDataObjectList = (List<Object>) columnMetadataMap.get(COLUMNDATALISTKEY);
        if(columnDataObjectList == null || columnDataObjectList.size() == 0) 
        {
            output.writeInt(-1);
        } 
        else 
        { 
            output.writeInt(columnDataObjectList.size());
        }
        if(columnDataObjectList != null && columnDataObjectList.size() > 0)
        {
            for (Object obj : columnDataObjectList)
            {
                ColumnMetadata columnMetadata = (ColumnMetadata) obj;
                output.writeObject(columnMetadata);
            }
        }


    }

    public static void main(String[] args) throws IOException, ClassNotFoundException
    {
        
        Metadata m1 = new Metadata();
        m1.setCount(1);
        m1.setRefreshTimeStamp("20120101000000");
        m1.setSchemaName("schema");
        m1.setTableName("table");
        Map<String, Object> theColumnDataMap = m1.getColumnMetadataMap();
        List<Object> columnDataObjectList = new ArrayList<Object>();
        ColumnMetadata cmd = new ColumnMetadata();
        cmd.setColumnLabel("columnLabel");
        cmd.setColumnName("columnName");
        cmd.setColumnType(11);
        cmd.setColumnTypeName("columnTypeName");
        columnDataObjectList.add(cmd);
        theColumnDataMap.put(COLUMNDATALISTKEY, columnDataObjectList);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream so = new ObjectOutputStream(baos);
        so.writeObject(m1);
        so.flush();
        System.out.println(m1);
        
        byte[] theData = baos.toByteArray();

        ByteArrayInputStream bais = new ByteArrayInputStream(theData);
        ObjectInputStream ois = new ObjectInputStream(bais);
        Metadata m2 = (Metadata) ois.readObject();
        System.out.println(m2);
        
        
        baos = new ByteArrayOutputStream();
        so = new ObjectOutputStream(baos);
        so.writeObject(m2);
        so.flush();
        
        theData = baos.toByteArray();

        bais = new ByteArrayInputStream(theData);
        ois = new ObjectInputStream(bais);
        Metadata m3 = (Metadata) ois.readObject();
        System.out.println(m3);


    }

    @Override
    public String toString()
    {
        return "Metadata [refreshTimeStamp=" + refreshTimeStamp + ", count=" + count + ", schemaName=" + schemaName
                + ", tableName=" + tableName + ", columnMetadataMap=" + columnMetadataMap + "]";
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((columnMetadataMap == null) ? 0 : columnMetadataMap.hashCode());
        result = prime * result + count;
        result = prime * result + ((refreshTimeStamp == null) ? 0 : refreshTimeStamp.hashCode());
        result = prime * result + ((schemaName == null) ? 0 : schemaName.hashCode());
        result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Metadata other = (Metadata) obj;
        if (columnMetadataMap == null)
        {
            if (other.columnMetadataMap != null)
                return false;
        } else if (!columnMetadataMap.equals(other.columnMetadataMap))
            return false;
        if (count != other.count)
            return false;
        if (refreshTimeStamp == null)
        {
            if (other.refreshTimeStamp != null)
                return false;
        } else if (!refreshTimeStamp.equals(other.refreshTimeStamp))
            return false;
        if (schemaName == null)
        {
            if (other.schemaName != null)
                return false;
        } else if (!schemaName.equals(other.schemaName))
            return false;
        if (tableName == null)
        {
            if (other.tableName != null)
                return false;
        } else if (!tableName.equals(other.tableName))
            return false;
        return true;
    }

    @Override
    public int compareTo(Metadata other)
    {
        String local = getRefreshTimeStamp();
        String external = other.getRefreshTimeStamp();
        return local.compareTo(external);
    }

    @Override
    public Map<String, Object> getDataMap()
    {
        return columnMetadataMap;
    }

    @Override
    public void removeIdenticalColumn(String columnName, Object latestValue)
    {
        Object lastValue = columnMetadataMap.get(columnName);
        if (lastValue == null)
        {
            return;
        }
        if (lastValue.equals(latestValue))
        {
            columnMetadataMap.remove(columnName);
        }

    }



}