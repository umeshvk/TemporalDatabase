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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mvdb.data.GenericDataRecord;
import com.mvdb.data.MultiVersionRecord;

@SuppressWarnings("deprecation")
public class TimeSliceSerde implements SerDe
{
    private static Logger logger = LoggerFactory.getLogger(TimeSliceSerde.class);

    int                   numColumns;
    StructObjectInspector rowOI;
    ArrayList<Object>     row;

    BytesWritable         serializeBytesWritable;
    // NonSyncDataOutputBuffer barrStr;
    // TypedBytesWritableOutput tbOut;

    // NonSyncDataInputBuffer inBarrStr;
    // TypedBytesWritableInput tbIn;

    List<String>          columnNames;
    List<TypeInfo>        columnTypes;
    Map<String, Integer>  columnPosition = new HashMap<String, Integer>();
    
    
    
    Date sliceDate = null;


    @Override
    public Object deserialize(Writable blob) throws SerDeException
    {

        System.out.println("deserialize");
        BytesWritable bw = (BytesWritable) blob;
        // inBarrStr.reset(data.getBytes(), 0, data.getLength());
        for (int i = 0; i < columnNames.size(); i++)
        {
            // row.set(i, deserializeField(tbIn, columnTypes.get(i),
            // row.get(i)));
            row.set(i, null);
        }
        try
        {
            byte[] bytes = bw.getBytes();
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            MultiVersionRecord mv = (MultiVersionRecord)ois.readObject();
            Date dateSlice = sliceDate != null ? sliceDate : new Date();             
            System.out.println("dateSlice:" + dateSlice);
            GenericDataRecord record = null; 
            GenericDataRecord result = (GenericDataRecord)mv.getVersion(mv.getVersionCount()-1);
            for(int i = mv.getVersionCount()-2;i>=0;i--)
            {
//                if(result.isDeleted())
//                {
//                    throw new SerDeException("Found deleted row");
//                }
                if(result.getTimestampLongValue() <= dateSlice.getTime())
                {
                    break;
                }
                //System.out.println("result before merge:" + result.toString());
                record = (GenericDataRecord)mv.getVersion(i);
                //System.out.println("record to merge:" + record.toString());
                result.merge(record);
                //System.out.println("result after merge:" + result.toString());

            }
            //System.out.println("result final:" + result.toString());
            
            
            Map<String, Object> dataMap = result.getDataMap();
            Iterator<String> keySetIter = dataMap.keySet().iterator();
            while(keySetIter.hasNext())
            {
                String key = keySetIter.next();
                Integer pos = columnPosition.get(key);
                if(pos == null)
                {
                    //Table does not care about this data.
                    continue;
                }
                Object value = dataMap.get(key);
                //System.out.println("key:" + key + ", value:" + value);
                Object tv = translate(value);
                //System.out.println(String.format(">>>>>%s %d %s", key, pos, tv));
                
                row.set(pos, tv);
            }
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (ClassNotFoundException e)
        {
            e.printStackTrace();
        }
        catch (Throwable e)
        {
            e.printStackTrace();
        }

        return row;
    }

    
    private Object translate(Object value)
    {
        Object newValue = null; 
//        System.out.println("Translation Input:" + value); 
//        System.out.println("Translation Input Type:" + value.getClass().toString()); 
        if(value instanceof java.lang.Integer)
        {
            newValue = new IntWritable((Integer)value);
        }
        else if(value instanceof java.lang.Long)
        {
            newValue = new LongWritable(((Long)value).intValue());
        }
        else if(value instanceof java.util.Date)
        {
            String dateString =  HiveUtil.getHiveTimestamp((Date)value);  //sdf.format((Date)value);
            newValue = new Text(dateString);
        }
        else if(value instanceof java.lang.String)
        {
            newValue = new Text(value.toString());
        } 
        else if(value instanceof java.math.BigDecimal)
        {
            newValue = new org.apache.hadoop.hive.serde2.io.DoubleWritable(((java.math.BigDecimal)value).doubleValue());
        }
        else if(value instanceof java.lang.Boolean)
        {
            newValue = new BooleanWritable(((Boolean)value).booleanValue());
        }
        else 
        {
            System.out.println("Unsupported type:" + value.getClass().toString()); 
            newValue = "test"; 
        }
//        System.out.println("Translation Result:" + newValue);          
        return newValue;
        
        
    }
    
    @Override
    public ObjectInspector getObjectInspector() throws SerDeException
    {
        return rowOI;
    }

    @Override
    public SerDeStats getSerDeStats()
    {
        return null;
    }

    @Override
    public void initialize(Configuration conf, Properties tbl) throws SerDeException
    {
        System.out.println("initialize");
        Object sliceDateStr = conf.get("sliceDate");
        
        sliceDate =  HiveUtil.getDateFromHiveTimeStamp((String)sliceDateStr); 
        //sdf.parse((String) sliceDateStr);
        System.out.println("sliceDate=" + sliceDate);
        
        serializeBytesWritable = new BytesWritable();
        // barrStr = new NonSyncDataOutputBuffer();
        // tbOut = new TypedBytesWritableOutput(barrStr);
        //
        // inBarrStr = new NonSyncDataInputBuffer();
        // tbIn = new TypedBytesWritableInput(inBarrStr);

        // Read the configuration parameters
        String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
        String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);

        columnNames = Arrays.asList(columnNameProperty.split(","));
        for(int i=0;i<columnNames.size();i++)
        {
            columnPosition.put(columnNames.get(i), i);
        }
        columnTypes = null;
        if (columnTypeProperty.length() == 0)
        {
            columnTypes = new ArrayList<TypeInfo>();
        } else
        {
            columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        }

        assert columnNames.size() == columnTypes.size();
        numColumns = columnNames.size();

        // All columns have to be primitive.
        for (int c = 0; c < numColumns; c++)
        {
            if (columnTypes.get(c).getCategory() != Category.PRIMITIVE)
            {
                throw new SerDeException(getClass().getName() + " only accepts primitive columns, but column[" + c
                        + "] named " + columnNames.get(c) + " has category " + columnTypes.get(c).getCategory());
            }
        }

        // Constructing the row ObjectInspector:
        // The row consists of some string columns, each column will be a java
        // String object.
        List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(columnNames.size());
        for (int c = 0; c < numColumns; c++)
        {
            columnOIs.add(TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(columnTypes.get(c)));
        }

        // StandardStruct uses ArrayList to store the row.
        rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);

        // Constructing the row object, etc, which will be reused for all rows.
        row = new ArrayList<Object>(numColumns);
        for (int c = 0; c < numColumns; c++)
        {
            row.add(null);
        }
    }

    @Override
    public Class<? extends Writable> getSerializedClass()
    {
        return BytesWritable.class;
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException
    {
        // try {
        // barrStr.reset();
        // StructObjectInspector soi = (StructObjectInspector) objInspector;
        // List<? extends StructField> fields = soi.getAllStructFieldRefs();
        //
        // for (int i = 0; i < numColumns; i++) {
        // Object o = soi.getStructFieldData(obj, fields.get(i));
        // ObjectInspector oi = fields.get(i).getFieldObjectInspector();
        // serializeField(o, oi, row.get(i));
        // }
        //
        // // End of the record is part of the data
        // tbOut.writeEndOfRecord();
        //
        // serializeBytesWritable.set(barrStr.getData(), 0,
        // barrStr.getLength());
        // } catch (IOException e) {
        // throw new SerDeException(e.getMessage());
        // }
        // return serializeBytesWritable;
        serializeBytesWritable.set(new byte[] {}, 0, 0);
        return serializeBytesWritable;

    }

}
