from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Create Spark session
    spark = SparkSession.builder \
        .appName("app1") \
        .getOrCreate()

    # Read input file
    rdd1 = spark.sparkContext.textFile("D:/input/data.txt")

    # Step 1: Split each line into words
    # Input:  "hello world"
    # Output: ["hello", "world"]
    rdd2 = rdd1.flatMap(lambda x: x.split(" "))

    # Step 2: Convert each word into (word, 1)
    # Input:  ["hello", "hello"]
    # Output: [("hello",1), ("hello",1)]
    rdd3 = rdd2.map(lambda x: (x, 1))

    # Step 3: Reduce by key to count the occurrences
    # Input:  ("hello", [1,1,1])
    # Output: ("hello", 3)
    rdd4 = rdd3.reduceByKey(lambda x, y: x + y)

    # Print Hadoop version just like in your screenshot
    print(f"Hadoop version = {spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")

    # Collect and print results
    for ele in rdd4.collect():
        print(ele)

    # Stop Spark session
    spark.stop()
    