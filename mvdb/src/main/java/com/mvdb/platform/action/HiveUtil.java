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

import java.text.SimpleDateFormat;
import java.util.Date;

public class HiveUtil
{
    private static SimpleDateFormat hiveTimeStampFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    
    public static String getHiveTimestamp(Date date)
    {
        return hiveTimeStampFormatter.format(date);
    }
    
    public static Date getDateFromHiveTimeStamp(String timestamp)
    {
        try
        {
            if(timestamp == null || timestamp.toLowerCase().startsWith("null"))
            {
                return null; 
            }
            return hiveTimeStampFormatter.parse(timestamp);
        }        
        catch (Throwable e)
        {            
            e.printStackTrace();
            return null;
        }
    }

}
