Temporal Database Notes:
=======================


What type of user needs it?
---------------------------

A user that needs to see the change in data over time. Assume that this user has data in a relational database. At any point in time 
the user can see what is in the database right now using an SQL query. What if we could add an "as_of_date <date>" clause to the query and and the 
database is able to tell us the result of that query on a certain date in the past. In other words this is a time machine that can only go backwards. 
Specifically this is not predictive in nature. That will require another project. 

To give an example a customer may have an account with salesforce.com or some other vendor. The customer has access to the underlying database and is 
able to get hourly or daily snapshots. This project is able to create deltas by comparing successive snapshots and store the deltas in a datastore much 
like a version control system. When responding to an as_of_date query the records would have to be re-constructed for that date and the resulting
record will be fed to the DAG that is produced by the SQL Query Parser. 

This type of querying enables an end user to observe the changes in data over time and enables analytic applications that rely on a JDBC client to access
data in the Temporal Database.


How to test this project?
-------------------------

Navigate to the usage-details  directory
ls z*sh
This will give a list of three shell scripts
They will point to other shell scripts. 

Can you provide an overview of the code(a high level walk thru)?
---------------------------------------------------------------
Yes I will be doing that this week or next. I may have a chance to do a presentationat work in which case I may put up a video. But this is currently low priority.
Here is the idea in brief: 
1. Simulate customer relational database instance in Postgres.(It could have been any RDBMS)
2. Initialize the schema in Postgres.
3. Initialize the data in the schema table(s).
4. Take a snapshot of data as a set of binary files. 
5. Simulate data changes.
6. Take a snapshot of the modified records in table. 
7. Repeat Steps 5 and 6 any number of times to simulate the daily snapshot cycle. 
8. Merge the deltas from step 5 and 6 to the destination in HDFS. 
9. The project uses two destinations in HDFS. One is active and the other is passive. Step 8 merges data to the passive location.
10. After Step 9 the active and passive locations are interchanged. Active becomes passive and passive becomes active. 
11. Runtime queries are directed to the active location. 
12. Hive uses Serde to deserialize the records in HDFS files and re-construct the record on the basis of the provided as_of_date. 
13. The Hive query results can be used for analytics. 
14. The Hive queries can be run over a JDBC client. Thus the results are accesible in any Java application.
15. As an aside the Hive metadata schema is also setup in Postgres.


What is the original inspiration of the system?
-----------------------------------------------

I worked on a system that provided similar functionality. But it was developed over 10 years and required in expensive storage solution. 
I wanted to see what I could put together in 1 month using modern technology. 

Would you call this project a success?
--------------------------------------

I am not satisfied with the query speed in Hive. I am hearing of recent gains of 10x in Hive. I need to go back to take a second look as to how this software performs in the new Hive. 
