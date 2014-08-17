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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class MultiVersionRecord implements Externalizable
{
    private static final long serialVersionUID = 1L;
    List<AnyRecord> versionList;
      
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append("[");
        sb.append("{\"count\" : " + versionList.size() + "}, ");
        String nl = System.getProperty("line.separator");
        for(AnyRecord gdr : versionList)
        {
            sb.append(nl);
            sb.append(gdr);
            sb.append(",");
        }
        sb.setLength(sb.length()-1);
        sb.append("]");
        
        return sb.toString();
        
    }
    
    public MultiVersionRecord()
    {
        versionList = new ArrayList<AnyRecord>();               
    }
        
    public AnyRecord getLatestVersion()
    {
        if(versionList.size() == 0)
        {
            return null; 
        }
        return versionList.get(versionList.size()-1);
    }
    
    public void addLatestVersion(AnyRecord latestVersion)
    {
        addLatestVersion(versionList, latestVersion);
    }
    
    public int getVersionCount()
    {
        return versionList.size(); 
    }
    
    public AnyRecord getVersion(int pos)
    {
        return versionList.get(pos);
    }
    
    private void addLatestVersion(List<AnyRecord> currentList, AnyRecord latestVersion)
    {
        if(currentList.size() == 0)
        {
            currentList.add(latestVersion);
            return;
        }
        
        AnyRecord lastVersion = currentList.get(currentList.size()-1);
        Map<String, Object> map = latestVersion.getDataMap();
        Iterator<String> iter = map.keySet().iterator();
        while(iter.hasNext()) { 
            String columnName = iter.next();
            Object latestValue = map.get(columnName);
            lastVersion.removeIdenticalColumn(columnName, latestValue);
        }
        currentList.add(latestVersion);            
        
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        int size = in.readInt();
        for(int i=0;i<size;i++)
        {
            AnyRecord recordVersion = (AnyRecord)in.readObject();
            versionList.add(recordVersion);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        int size = versionList.size();
        out.writeInt(size);
        for(int i=0;i<size;i++)
        {
            AnyRecord recordVersion = versionList.get(i);
            out.writeObject(recordVersion);
        }
    }

}
