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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GenericIdRecord implements IdRecord
{
    private static final long serialVersionUID = 1L;

    private static Logger logger = LoggerFactory.getLogger(GenericIdRecord.class);

    Object originalKeyValue; 
    String mvdbKeyValue; 
    String refreshTimeStamp;
   
    public GenericIdRecord(Object originalKeyValue, MvdbKeyMaker mvdbKeyMaker, String refreshTimeStamp)
    {
        this.originalKeyValue = originalKeyValue;
        this.mvdbKeyValue = mvdbKeyMaker.makeKey(originalKeyValue);
        this.refreshTimeStamp = refreshTimeStamp;
    }


    public GenericIdRecord()
    {
    }

    public Object getKeyValue()
    {
        return originalKeyValue;
    }

    @Override
    public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException
    {
        this.originalKeyValue = input.readObject();
        this.mvdbKeyValue = (String)input.readObject(); 
        this.refreshTimeStamp = (String)input.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput output) throws IOException
    {        
        output.writeObject(originalKeyValue);
        output.writeObject(mvdbKeyValue);     
        output.writeObject(refreshTimeStamp);  
    }
    
    public Date getMvdbUpdateTime()
    {      
        return DataUtils.getDate(refreshTimeStamp);
    }


    public String getRefreshTimeStamp()
    {
        return refreshTimeStamp;
    }


    public void setRefreshTimeStamp(String refreshTimeStamp)
    {
        this.refreshTimeStamp = refreshTimeStamp;
    }


    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append("{");
        sb.append("\"originalKeyValue\"");
        sb.append(" : ");
        sb.append("\"" + originalKeyValue + "\"");
        sb.append(", ");
        sb.append("\"mvdbKeyValue\"");
        sb.append(" : ");
        sb.append("\"" + mvdbKeyValue + "\"");
        sb.append(", ");
        sb.append("\"mvdbUpdateTime\"");
        sb.append(" : ");
        sb.append("\"" + getMvdbUpdateTime() + "\"");
        sb.append(", ");
        sb.append("\"refreshTimeStamp\"");
        sb.append(" : ");
        sb.append("\"" + refreshTimeStamp + "\"");
        
        sb.append("}");
        
        return sb.toString();
    }
    
    public Object getOriginalKeyValue()
    {
        return originalKeyValue;
    }


    public void setOriginalKeyValue(Object originalKeyValue)
    {
        this.originalKeyValue = originalKeyValue;
    }


    public String getMvdbKeyValue()
    {
        return mvdbKeyValue;
    }


    public void setMvdbKeyValue(String mvdbKeyValue)
    {
        this.mvdbKeyValue = mvdbKeyValue;
    }


    @Override
    public long getTimestampLongValue()
    {
        return getMvdbUpdateTime().getTime();
    }
    
    @Override
    public int compareTo(IdRecord idRecord)
    {            
        Long local = new Long(this.getTimestampLongValue());
        Long external = new Long(idRecord.getTimestampLongValue());        
        return local.compareTo(external);
    }


    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((mvdbKeyValue == null) ? 0 : mvdbKeyValue.hashCode());
        result = prime * result + ((originalKeyValue == null) ? 0 : originalKeyValue.hashCode());
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
        GenericIdRecord other = (GenericIdRecord) obj;
        if (mvdbKeyValue == null)
        {
            if (other.mvdbKeyValue != null)
                return false;
        } else if (!mvdbKeyValue.equals(other.mvdbKeyValue))
            return false;
        if (originalKeyValue == null)
        {
            if (other.originalKeyValue != null)
                return false;
        } else if (!originalKeyValue.equals(other.originalKeyValue))
            return false;
        return true;
    }

    @Override
    public Map<String, Object> getDataMap()
    {
        throw new RuntimeException("Operation not supported");
    }

    @Override
    public void removeIdenticalColumn(String columnName, Object latestValue)
    {
        throw new RuntimeException("Operation not supported"); 
    }

}
