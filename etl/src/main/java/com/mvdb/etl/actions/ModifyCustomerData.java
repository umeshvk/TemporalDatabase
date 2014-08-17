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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.mvdb.etl.dao.OrderDAO;
import com.mvdb.etl.model.Order;
import com.mvdb.etl.util.RandomUtil;

public class ModifyCustomerData  implements IAction
{
    private static Logger logger = LoggerFactory.getLogger(ModifyCustomerData.class);


    
    public static void main(String[] args)
    {
        
        ActionUtils.assertEnvironmentSetupOk();
        ActionUtils.assertFileExists("~/.mvdb", "~/.mvdb missing. Existing.");
        ActionUtils.assertFileExists("~/.mvdb/status.InitCustomerData.complete", "300init-customer-data.sh not executed yet. Exiting");
        //This check is not required as data can be modified any number of times
        //ActionUtils.assertFileDoesNotExist("~/.mvdb/status.ModifyCustomerData.complete", "ModifyCustomerData already done. Start with 100init.sh if required. Exiting");
        ActionUtils.setUpInitFileProperty();
        ActionUtils.createMarkerFile("~/.mvdb/status.ModifyCustomerData.start", true);
        
        logger.error("error");
        logger.warn("warning");
        logger.info("info");
        logger.debug("debug");
        logger.trace("trace");
        
        String customerName = null; 
        String modifyAction = null;
//        String modifyCountStr = null;
        //Date startDate  = null;
        //Date endDate  = null;
        final CommandLineParser cmdLinePosixParser = new PosixParser();
        final Options posixOptions = constructPosixOptions();
        CommandLine commandLine;
        try
        {
            commandLine = cmdLinePosixParser.parse(posixOptions, args);
            if (commandLine.hasOption("customerName"))
            {
                customerName = commandLine.getOptionValue("customerName");
            }
            if (commandLine.hasOption("modifyAction"))
            {
                modifyAction = commandLine.getOptionValue("modifyAction");
            }
//            if (commandLine.hasOption("modifyCount"))
//            {
//                modifyCountStr = commandLine.getOptionValue("modifyCount");
//            }
//            if(modifyCountStr == null)
//            {
//                modifyCountStr = "1"; 
//            }
            
        } catch (ParseException parseException) // checked exception
        {
            System.err
                    .println("Encountered exception while parsing using PosixParser:\n" + parseException.getMessage());
        }

        if (customerName == null)
        {
            System.err.println("customerName has not been specified.  Aborting...");
            System.exit(1);
        }
        
        if (modifyAction != null)
        {
            if(Action.DELETE.getName().equals(modifyAction) == false && Action.UNDELETE.getName().equals(modifyAction) == false)
            {
                System.err.println("modifyAction must be <Delete> or <Undelete>.  Aborting...");
                System.exit(1); 
            } 
        }
        
           
        modifyCustomerData(customerName, modifyAction, 1L);
        
        ActionUtils.createMarkerFile("~/.mvdb/status.ModifyCustomerData.complete", true);
    }
    
    public static void modifyCustomerData(String customerName, Action modifyAction, Long actionId)
    {
        modifyCustomerData(customerName, modifyAction.getName(), actionId);
    }
    
    public static void modifyCustomerData(String customerName)
    {
        modifyCustomerData(customerName, Action.NOOP, -1L);
    }
    
    public static void modifyCustomerData(String customerName, String modifyAction, Long actionId)
    {
        ApplicationContext context = Top.getContext();

        final OrderDAO orderDAO = (OrderDAO) context.getBean("orderDAO");

        
        long maxId = orderDAO.findMaxId();
        long totalOrders = orderDAO.findTotalRecords();
        
        long modifyCount = (long)(totalOrders * 0.1);
        if(modifyCount == 0L)
        {
            modifyCount = 1L;
        }
       

        long lastUsedEndTime = ActionUtils.getConfigurationValueLong(customerName, ConfigurationKeys.LAST_USED_END_TIME);
        Date startDate1 = new Date();
        startDate1.setTime(lastUsedEndTime + 1000 * 60 * 60 * 24 * 1);
        Date endDate1 = new Date(startDate1.getTime() + 1000 * 60 * 60 * 24 * 1);
        //We force this value so that all the changes made up to  startDate1 will be picked up in the ExtractDBChanges stage.
        ActionUtils.setConfigurationValue(customerName, ConfigurationKeys.LAST_REFRESH_TIME, String.valueOf(startDate1.getTime()-1));
        
        List<Long> orderIdList = getRandomOrderIds(orderDAO, modifyCount);
        Long modifiedId = -1L; 
        for(int i=0;i<orderIdList.size();i++)
        {
            modifiedId = orderIdList.get(i);
            modify(orderDAO, modifiedId, startDate1,  endDate1);
        }
        
        //orderIdList = getRandomOrderIds(orderDAO, 1);
        if(Action.DELETE.getName().equals(modifyAction))
        {
            handleDelete(orderDAO, actionId);
        }
        else if(Action.UNDELETE.getName().equals(modifyAction))
        {
            handleUndelete(orderDAO, actionId, modifiedId);
        }
        
        
        ActionUtils.setConfigurationValue(customerName, ConfigurationKeys.LAST_USED_END_TIME, String.valueOf(endDate1.getTime()));
        logger.info("Modified " + modifyCount + " orders");
    }

    private static List<Long> getRandomOrderIds(OrderDAO orderDAO, long count)
    {
        List<Long> idList = orderDAO.findAllIds();
        List<Long> retList = new ArrayList<Long>();
        //In some cases we may hit the same orderId twice. But it is not critical right now.  
        for(long i=0;i<count;i++)
        {
            int orderIdIndex = (int)Math.floor((Math.random() * idList.size()));        
            Long orderId = idList.get(orderIdIndex);
            retList.add(orderId);
        }
        
        return retList;
    }
    
//    / 
    private static void modify(OrderDAO orderDAO, long orderId, Date startDate1, Date endDate1)
    {
           
        logger.info("Modify Id " + orderId + " in orders");
        
        Date updateDate = RandomUtil.getRandomDateInRange(startDate1, endDate1); 
        Order theOrder = orderDAO.findById(orderId);
        theOrder.setNote(RandomUtil.getRandomString(4));
        theOrder.setUpdateTime(updateDate);
        theOrder.setSaleCode(RandomUtil.getRandomInt());
        orderDAO.update(theOrder);
    }

    private static void handleUndelete(OrderDAO orderDAO, long orderId, long referenceOrderId)
    {
        if(orderDAO.findById(orderId) != null) { 
            logger.info("Cannot undelete existing Id " + orderId + " in orders");
            return;
        }
        Order referenceOrder = orderDAO.findById(referenceOrderId);
        if(referenceOrder == null) { 
            logger.info("Cannot undelete using non-existent Id " + orderId + " in orders");
            return;
        }
        Date updateTime = referenceOrder.getUpdateTime(); 
        Date createTime = referenceOrder.getCreateTime();
        Order undeleteOrder = new Order();
        undeleteOrder.setOrderId(orderId);
        undeleteOrder.setNote(RandomUtil.getRandomString(4));
        undeleteOrder.setSaleCode(RandomUtil.getRandomInt());
        undeleteOrder.setUpdateTime(updateTime);
        undeleteOrder.setCreateTime(createTime);
        orderDAO.insert(undeleteOrder);        
    }

    private static void handleDelete(OrderDAO orderDAO, long orderId)
    {    
        if(orderDAO.findById(orderId) == null) { 
            logger.info("Cannot delete non-existent Id " + orderId + " in orders");
            return;
        }
        orderDAO.deleteById(orderId);
    }

    public static Options constructPosixOptions()
    {
        final Options posixOptions = new Options();
        posixOptions.addOption("customerName", true, "Customer Name");
        posixOptions.addOption("modifyAction", true, "Modify Action");
//      posixOptions.addOption("modifyCount", true, "Modify Count");

        return posixOptions;
    }
}
