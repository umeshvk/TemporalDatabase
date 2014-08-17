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
package com.mvdb.etl.actions;

import java.util.Date;

import org.codehaus.jettison.json.JSONException;

import com.mvdb.data.DataUtils;



/**
 * Uses the four classes InitDB{1}, InitCustomerData{1}, ExtractDBChanges{1}, (ModifyCustomerData, ExtractDBChanges){0,} to create a VersionedCustomer 
 * @author umesh
 *
 */
public class InitVersionedCustomer
{

    /**
     * @param args
     * @throws JSONException 
     * 
     */
    public static void main(String[] args) throws JSONException
    {
        String customerName = "alpha"; 
        int batchCount = 1; 
        int batchSize = 10; 
        Date recordCreationTimeStartDate = DataUtils.getDate("20020115050607"); 
        Date recordCreationTimeEndDate = DataUtils.getDate("20030115050607"); 
             
        ActionUtils.setUpInitFileProperty();
        InitDB.initDB();
        
        InitCustomerData.initCustomerData(customerName, batchCount, batchSize, recordCreationTimeStartDate, recordCreationTimeEndDate);
        ExtractDBChanges.extractDbchanges(customerName);
        
        
        ModifyCustomerData.modifyCustomerData(customerName, Action.DELETE, 1L);
        ExtractDBChanges.extractDbchanges(customerName);
        
        
        ModifyCustomerData.modifyCustomerData(customerName, Action.NOOP, 1L);
        ExtractDBChanges.extractDbchanges(customerName);
        
        
        ModifyCustomerData.modifyCustomerData(customerName, Action.NOOP, 1L);
        ExtractDBChanges.extractDbchanges(customerName);
        
        
        ModifyCustomerData.modifyCustomerData(customerName, Action.UNDELETE, 1L);
        ExtractDBChanges.extractDbchanges(customerName);
        
        ModifyCustomerData.modifyCustomerData(customerName, Action.NOOP, 1L);
        ExtractDBChanges.extractDbchanges(customerName);
       
    }

}
