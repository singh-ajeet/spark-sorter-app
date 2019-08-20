# spark-sorter-app
A simple spark application to sort records by a column in CSV.

###### Example usage:
*spark-submit --class com.ajeet.bigdata.spark.SorterApp --master yarn --deploy-mode cluster /mnt/pb/spark-sorter-app-1.0-SNAPSHOT.jar  /user/hadoop/testData_1M.txt /user/pbuser/sorted-output/ '|' 7*

