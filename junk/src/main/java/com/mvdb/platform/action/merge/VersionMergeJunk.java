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
package com.mvdb.platform.action.merge;

import org.apache.hadoop.conf.Configuration;
import org.springframework.context.ApplicationContext;

import com.mvdb.etl.actions.Top;
import com.mvdb.etl.dao.GenericDAO;
import com.mvdb.etl.data.Metadata;

public class VersionMergeJunk
{

    
    private static void testMD(Configuration conf)
    {
        ApplicationContext context = Top.getContext();
        GenericDAO genericDAO = (GenericDAO)context.getBean("genericDAO");
                
        //Metadata md = genericDAO.readMetadata(new File("/home/umesh/.mvdb/etl/data/alpha/20030115050607/schema-orderlineitem.dat").toURI().toString(), conf1);
        Metadata md = genericDAO.readMetadata("hdfs://localhost:9000/data/alpha/20030115050607/schema-orderlineitem.dat", conf);

        
    }

    
    
}


