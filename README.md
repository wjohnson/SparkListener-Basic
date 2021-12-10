# SparkListener-Basic
A Spark Listener implemented in Java 

This is part of a sample that helps you demonstrate how to debug a Spark Listener or Application.

This sample is geared toward Scala 2.12 and Spark 3.2.0 but can be updated by changing `./lib/build.gradle` sparkVersion and scalaVersion.

## Using VS Code for Debugging

* Open this project in VS Code with the Java Extension Pack and wait for the Java project to be successfully imported.
* Execute `gradle build` to get the jar.
* Run `export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005` to store the environment variable that `spark-shell` will expect to start the debugger.
* Execute `spark-shell --conf spark.driver.extraClassPath=/PATH/TO/THIS/DIR/lib/build/libs/customListener-1.0-SNAPSHOT.jar --conf spark.extraListeners=listener.MyListener`
  * As the spark shell starts up, you should see this message in the logs `Listening for transport dt_socket at address: 5005`
* Connect with VS Code debugger's "Attach to Remote Program" option.
* Set your breakpoints in the Java project
* Execute a spark job like this:

```scala
spark.sparkContext.setLogLevel("ERROR") // This lets you see log messages in stdout
import spark.implicits._
val columns = Seq("city","population")
val data = Seq(("Chicago", "20000"), ("NYC", "100000"), ("Detroit", "3000"))
val rdd = spark.sparkContext.parallelize(data)
val df = rdd.toDF()
df.printSchema()
val results = df.collect()
```
