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

import com.mvdb.data.DataUtils;
import com.mvdb.etl.dao.ConfigurationDAO;
import com.mvdb.etl.dao.OrderDAO;
import com.mvdb.etl.dao.OrderLineItemDAO;
import com.mvdb.etl.model.Order;
import com.mvdb.etl.model.OrderLineItem;
import com.mvdb.etl.monitoring.TimedExecutor;
import com.mvdb.etl.util.RandomUtil;

public class InitCustomerData  implements IAction
{
    private static Logger logger = LoggerFactory.getLogger(InitCustomerData.class);
    
    public static void main(String[] args)
    {
        ActionUtils.assertEnvironmentSetupOk();
        ActionUtils.assertFileExists("~/.mvdb", "~/.mvdb missing. Existing.");
        ActionUtils.assertFileExists("~/.mvdb/status.InitDB.complete", "200initdb.sh not executed yet. Exiting");
        ActionUtils.assertFileDoesNotExist("~/.mvdb/status.InitCustomerData.complete", "InitCustomerData already done. Start with 100init.sh if required. Exiting");
        ActionUtils.setUpInitFileProperty();
        ActionUtils.createMarkerFile("~/.mvdb/status.InitCustomerData.start");
        logger.error("error");
        logger.warn("warning");
        logger.info("info");
        logger.debug("debug");
        logger.trace("trace");
        
        Date startDate  = null;
        Date endDate  = null;
        String customerName= null; 
        int batchCount = 0;
        int batchSize = 0;
        final CommandLineParser cmdLinePosixParser = new PosixParser();
        final Options posixOptions = constructPosixOptions();
        CommandLine commandLine;
        try
        {
            commandLine = cmdLinePosixParser.parse(posixOptions, args);
            if (commandLine.hasOption("customer"))
            {
                customerName = commandLine.getOptionValue("customer");
            }
            if (commandLine.hasOption("batchSize"))
            {
                String batchSizeStr = commandLine.getOptionValue("batchSize");
                batchSize = Integer.parseInt(batchSizeStr);
            }
            if (commandLine.hasOption("batchCount"))
            {
                String batchCountStr = commandLine.getOptionValue("batchCount");
                batchCount = Integer.parseInt(batchCountStr);
            }
            if (commandLine.hasOption("startDate"))
            {
                String startDateStr = commandLine.getOptionValue("startDate");
                startDate = DataUtils.getDate(startDateStr);
            }
            if (commandLine.hasOption("endDate"))
            {
                String endDateStr = commandLine.getOptionValue("endDate");
                endDate = DataUtils.getDate(endDateStr);
            }
        } catch (ParseException parseException) // checked exception
        {
            System.err
                    .println("Encountered exception while parsing using PosixParser:\n" + parseException.getMessage());
        }
        
        
        if (startDate == null)
        {
            System.err.println("startDate has not been specified with the correct format YYYYMMddHHmmss.  Aborting...");
            System.exit(1);
        }
        
        if (endDate == null)
        {
            System.err.println("endDate has not been specified with the correct format YYYYMMddHHmmss.  Aborting...");
            System.exit(1);
        }
        
        if (endDate.after(startDate) == false)
        {
            System.err.println("endDate must be after startDate.  Aborting...");
            System.exit(1);
        }
        
        initCustomerData(customerName, batchCount, batchSize, startDate, endDate);
        
        ActionUtils.createMarkerFile("~/.mvdb/status.InitCustomerData.complete");

    }

    public static void initCustomerData(String customerName, int batchCount, int batchSize, Date startDate,
            Date endDate)
    {
       
        ApplicationContext context = Top.getContext();

        
        final OrderDAO orderDAO = (OrderDAO) context.getBean("orderDAO"); 
        final OrderLineItemDAO orderLineItemDAO = (OrderLineItemDAO) context.getBean("orderLineItemDAO"); 
        final ConfigurationDAO configurationDAO = (ConfigurationDAO) context.getBean("configurationDAO");

        initOrderData(orderDAO, batchCount, batchSize, startDate, endDate);
        initOrderLineItemData(orderLineItemDAO, batchCount, batchSize, startDate, endDate);
        initConfiguration(configurationDAO, customerName, endDate);
        
        int total = orderDAO.findTotalRecords();
        System.out.println("Total : " + total);

        long max = orderDAO.findMaxId();
        System.out.println("maxid : " + max);
    }



    private static void initConfiguration(ConfigurationDAO configurationDAO, String customerName, Date endDate)
    {
        String schemaDescription = "{ ''root'' : [" + 
                        "{''table'' : ''orders'', ''keyColumn'' : ''order_id'', ''updateTimeColumn'' : ''update_time''} , " +
                        "{''table'' : ''order_line_item'', ''keyColumn'' : ''order_line_item_id'', ''updateTimeColumn'' : ''update_time''}" +
                        /**: Add one such line for every new table in the schema for the specified customer
                        "{''table'' : ''order_line_item'', ''keyColumn'' : ''order_line_item_id'', ''updateTimeColumn'' : ''update_time''}" +
                        **/ 
                                                "]}";
        String[] sqlArray = new String[] {
                "INSERT INTO configuration (customer, name, value, category, note) VALUES  ('" + customerName + "', 'last-used-end-time', '" + endDate.getTime() + "', '', '');",
                "INSERT INTO configuration (customer, name, value, category, note) VALUES  ('" + customerName + "', 'last-refresh-time', '0', '', '');",
                "INSERT INTO configuration (customer, name, value, category, note) VALUES  ('" + customerName + "', 'last-refresh-dirname', '00000000000000', '', '');",
                "INSERT INTO configuration (customer, name, value, category, note) VALUES  ('" + customerName + "." + "mv1" + "', 'last-copy-to-hdfs-dirname', '00000000000000', '', '');",
                "INSERT INTO configuration (customer, name, value, category, note) VALUES  ('" + customerName + "." + "mv1" + "', 'last-merge-to-mvdb-dirname', '00000000000000', '', '');",
                "INSERT INTO configuration (customer, name, value, category, note) VALUES  ('" + customerName + "." + "mv2" + "', 'last-copy-to-hdfs-dirname', '00000000000000', '', '');",
                "INSERT INTO configuration (customer, name, value, category, note) VALUES  ('" + customerName + "." + "mv2" + "', 'last-merge-to-mvdb-dirname', '00000000000000', '', '');",                
                "INSERT INTO configuration (customer, name, value, category, note) VALUES  ('" + customerName + "', 'refresh-lock', '0', '', '');",
                "INSERT INTO configuration (customer, name, value, category, note) VALUES  ('" + customerName + "', 'copy-to-hdfs-lock', '0', '', '');",
                "INSERT INTO configuration (customer, name, value, category, note) VALUES  ('" + customerName + "', 'merge-to-mvdb-lock', '0', '', '');", 
                "INSERT INTO configuration (customer, name, value, category, note) VALUES  ('" + customerName + "', 'active-db-dir', 'mv1', '', '');",
                "INSERT INTO configuration (customer, name, value, category, note) VALUES  ('" + customerName + "', 'schema-description', '" + schemaDescription + "', '', '');"
                };
        configurationDAO.executeSQl(sqlArray);        
    }

    private static void initOrderData(final OrderDAO orderDAO, final int batchCount, final int batchSize, final Date startDate, final Date endDate)
    {
        
        for (int batchIndex = 0; batchIndex < batchCount; batchIndex++)
        {
            final int batchIndexFinal = batchIndex;
            TimedExecutor te = new TimedExecutor() {

                @Override
                public void execute()
                {

                    
                    List<Order> orders = new ArrayList<Order>();
                    for (int recordIndex = 0; recordIndex < batchSize; recordIndex++)
                    {
                        Date createDate = RandomUtil.getRandomDateInRange(startDate, endDate);
                        Date updateDate = new Date();
                        updateDate.setTime(createDate.getTime());
                        Order order = new Order(orderDAO.getNextSequenceValue(), RandomUtil.getRandomString(5),
                                RandomUtil.getRandomInt(), createDate, updateDate);
                        orders.add(order);
                    }
                    orderDAO.insertBatch(orders);
                    System.out.println(String.format("Completed Order Batch %d of %d where size of batch is %s",
                            batchIndexFinal + 1, batchCount, batchSize));
                }

            };
            long runTime = te.timedExecute();
            System.out.println("Ran for seconds: " + runTime / 1000);
        }
        
    }
    
    private static void initOrderLineItemData(final OrderLineItemDAO orderLineItemDAO, 
            final int batchCount, final int batchSize, 
            final Date startDate, final Date endDate)
    {
        
        for (int batchIndex = 0; batchIndex < batchCount; batchIndex++)
        {
            final int batchIndexFinal = batchIndex;
            TimedExecutor te = new TimedExecutor() {

                @Override
                public void execute()
                {

                    
                    List<OrderLineItem> orderLineItemList = new ArrayList<OrderLineItem>();
                    for (int recordIndex = 0; recordIndex < batchSize; recordIndex++)
                    {
                        Date createDate = RandomUtil.getRandomDateInRange(startDate, endDate);
                        Date updateDate = new Date();
                        updateDate.setTime(createDate.getTime());
                        OrderLineItem orderLineItem = new OrderLineItem(orderLineItemDAO.getNextSequenceValue(), 
                                                                        RandomUtil.getRandomInt(1, 10), 
                                                                        RandomUtil.getRandomString(5), 
                                                                        RandomUtil.getRandomInt(1, 5), 
                                                                        RandomUtil.getRandomDouble(200.00), 
                                                                        createDate, 
                                                                        updateDate);
                        orderLineItemList.add(orderLineItem);
                    }
                    orderLineItemDAO.insertBatch(orderLineItemList);
                    System.out.println(String.format("Completed OrderLineItem Batch %d of %d where size of batch is %s",
                            batchIndexFinal + 1, batchCount, batchSize));
                }

            };
            long runTime = te.timedExecute();
            System.out.println("Ran for seconds: " + runTime / 1000);
        }
        
    }



    public static Options constructPosixOptions()
    {
        final Options posixOptions = new Options();
        posixOptions.addOption("customer", true, "Customer Name");
        posixOptions.addOption("batchCount", true, "Number of batches. Each batch is a transaction.");
        posixOptions.addOption("batchSize", true, "Number of records inserted in each batch");
        posixOptions.addOption("startDate", true, "Start Date");
        posixOptions.addOption("endDate", true, "End Date");
        return posixOptions;
    }
}




