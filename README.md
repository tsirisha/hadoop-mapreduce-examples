Hadoop Mapreduce Example using maven: Log file Analysis:
Processing based on date time so that number of different log messages based on each day is reported. 
Here in this example we need to group mapper keys on log type and group them per day. 

Example:

------------------------------------------
2014-11-07 T 12:00:00.000 ERROR	36

2014-11-07 T 12:00:00.000 INFO	1827

2014-11-07 T 12:00:00.000 WARN	57

2014-11-08 T 12:00:00.000 ERROR	18

2014-11-08 T 12:00:00.000 INFO	465

2014-11-08 T 12:00:00.000 WARN	27

-------------------------------------------------
