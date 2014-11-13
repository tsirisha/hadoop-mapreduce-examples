Hadoop Mapreduce Example using eclipse and maven: Log file Analysis:
This article gives brief overview of how to apply map reduce using Eclipse and maven to calculate number of log message types occurred on daily basis. 

Prerequisites:

•	Hadoop setup on virtual machine.

•	Java Version 6 Development & Runtime Environment.

•	Eclipse. 

•	Maven.

Instructions:

Maven setup on eclipse:

1.	Open Eclipse
2.	Go to Help -> Eclipse Marketplace
3.	Search by Maven
4.	Click "Install" button at "Maven Integration for Eclipse" section
5.	Follow the instruction step by step
To check Maven installation do the followings in Eclipse:
1.	Go to Window --> Preferences
2.	 Maven is listed at left panel.


Maven project using eclipse:

Go to eclipse create new maven project as follows 

select New ->Maven Project

Create custom key class (DateErrorWritable.java): In log file analysis as we need to group date and error type fields we need to create custom key class to hold error type and date fields. In order to create custom key class we need to implement WritableComparable interface.

Create Mapper and Reducer class(LogCounts.java).


Compiling and run instructions:

Go to eclipse workspace where project created run following command. It created jar file in dist directory of your project

mvn clean install

Run Hadoop jar going to dist directory.

hadoop jar logcounts.jar com.mapreduce.examples.LogCounts logfile.log mapperlog1



Output:

------------------------------------------
2014-11-07 T 12:00:00.000 ERROR	36

2014-11-07 T 12:00:00.000 INFO	1827

2014-11-07 T 12:00:00.000 WARN	57

2014-11-08 T 12:00:00.000 ERROR	18

2014-11-08 T 12:00:00.000 INFO	465

2014-11-08 T 12:00:00.000 WARN	27

-------------------------------------------------
