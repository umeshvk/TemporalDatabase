Temporal Database Notes:
=======================


What type of user needs it?
---------------------------

A user that needs to see the change in data over time. Assume that this user has data in a relational database. At any point in time 
the user can see what is in the database right now using an SQL query. What if we could add an "as_of_date <date>" clause to the query and and the 
databse is able to tell us the result of that query on a certain date in the past. In other words this is a time machine that can olny go backwards. 
Specifically this is not predictive in nature. That will require another project. 

To give an example a customer may have an account with salesforce.com or some other vendor. The customer has access to the underlying database and is 
able to get hourly or daily snapshots. This project is able to create deltas by comparing successive snapshots and store the deltas in a datastore much 
like a version contril of a file repository. When responding to an as_of_date query the records would have to be re-constructedfor that date and the resulting
record will be fed to the DAG that is produced by the SQL Query Parser. 

This type of querying enables a end user to observe the changes in data over time and enables analytic applications that rely on a JDBC client to access
data in the Temporal Database.

