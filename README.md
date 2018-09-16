#### We want to sum up Value(s) per Product per hour

E.g. input stream is <Date,Product,Value>
```
#Date,Product,Value
2016-05-16,A,1.0
2016-05-16,B,2.0
2016-05-16,A,3.0
2016-05-17,A,1.0
2016-05-17,B,1.0
```

Consume data
```
val df = spark.read.option("header","true").option("inferSchema","true").csv("products.csv")
```


Total value (Sum) each hour
```		
val df2 = df.groupBy(window(df.col("Date"), "1 day"), df.col("Product")).agg(sum("Value").alias("sum")).sort("Window", "Product")

df2.limit(4).show(false)
+------------------------------------------+-------+----------+                 
|window                                    |Product|sum(Value)|
+------------------------------------------+-------+----------+
|[2016-05-15 17:00:00, 2016-05-16 17:00:00]|A      |4.0       |
|[2016-05-15 17:00:00, 2016-05-16 17:00:00]|B      |2.0       |
|[2016-05-16 17:00:00, 2016-05-17 17:00:00]|A      |1.0       |
|[2016-05-16 17:00:00, 2016-05-17 17:00:00]|B      |8.0       |
+------------------------------------------+-------+----------+
```


Display product with max Sum per hour
```
import org.apache.spark.sql.expressions.Window

val w = Window.partitionBy($"window")


val df3 = df2.withColumn("maxSum", max("sum").over(w)).filter($"maxSum" === $"sum")

df3.show()
+--------------------+-------+---+------+
|              window|Product|sum|maxSum|
+--------------------+-------+---+------+
|[2016-05-16 17:00...|      B|8.0|   8.0|
|[2016-05-15 17:00...|      A|4.0|   4.0|
+--------------------+-------+---+------+

# df3.drop("sum").withColumnRenamed("maxSum", "MAX").show()
```


SQL approach
```
df.registerTempTable("ids")
val df = sqlContext.sql("select id, id % 3 as group_id from ids")
```
