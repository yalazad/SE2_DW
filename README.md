# Software Engineering + Data Wrangling with SQL (Combined Assessment)

## Create a Python 3 application must accommodate the following requirements: 
1. Gracefully handle the connection to the database server.
2. Replicate the algorithm of the specified stored function.
3. Replicate the algorithm of the trigger dbo.trg_refreshSurveyView for creating/altering the view whenever applicable.
4. For achieving (3) above, a persistence component (in any format you like: CSV, XML, JSON, etc.), storing the last
known surveys’ structures should be in place. It is not acceptable to just recreate the view every time: the trigger
behaviour must be replicated.
5. Of course, extract the “always-fresh” pivoted survey data, in a CSV file, adequately named.
