from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("Soccer Bronze Load") \
    .config("spark.jars", "C:/Users/Joshua Demetrioff/Documents/jars/postgresql-42.7.8.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


base_path = "C:/Users/Joshua Demetrioff/Documents/Soccer Project/raw-data/"

jdbc_url = "jdbc:postgresql://18.134.163.221:5432/testdb"

jdbc_props = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}


country_df = spark.read.csv(base_path + "Country.csv", header=True, inferSchema=True)
league_df = spark.read.csv(base_path + "League.csv", header=True, inferSchema=True)
match_df = spark.read.csv(base_path + "Match.csv", header=True, inferSchema=True)
player_attributes_df = spark.read.csv(base_path + "Player_Attributes.csv", header=True, inferSchema=True)
player_df = spark.read.csv(base_path + "Player.csv", header=True, inferSchema=True)
team_attributes_df = spark.read.csv(base_path + "Team_Attributes.csv", header=True, inferSchema=True)
team_df = spark.read.csv(base_path + "Team.csv", header=True, inferSchema=True)


country_df.printSchema()
league_df.printSchema()
match_df.printSchema()
player_attributes_df.printSchema()
player_df.printSchema()
team_attributes_df.printSchema()
team_df.printSchema()


country_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "soccer_db.country") \
    .option("user", jdbc_props["user"]) \
    .option("password", jdbc_props["password"]) \
    .option("driver", jdbc_props["driver"]) \
    .mode("overwrite") \
    .save()

league_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "soccer_db.league") \
    .option("user", jdbc_props["user"]) \
    .option("password", jdbc_props["password"]) \
    .option("driver", jdbc_props["driver"]) \
    .mode("overwrite") \
    .save()

match_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "soccer_db.match") \
    .option("user", jdbc_props["user"]) \
    .option("password", jdbc_props["password"]) \
    .option("driver", jdbc_props["driver"]) \
    .mode("overwrite") \
    .save()

player_attributes_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "soccer_db.player_attributes") \
    .option("user", jdbc_props["user"]) \
    .option("password", jdbc_props["password"]) \
    .option("driver", jdbc_props["driver"]) \
    .mode("overwrite") \
    .save()

player_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "soccer_db.player") \
    .option("user", jdbc_props["user"]) \
    .option("password", jdbc_props["password"]) \
    .option("driver", jdbc_props["driver"]) \
    .mode("overwrite") \
    .save()

team_attributes_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "soccer_db.team_attributes") \
    .option("user", jdbc_props["user"]) \
    .option("password", jdbc_props["password"]) \
    .option("driver", jdbc_props["driver"]) \
    .mode("overwrite") \
    .save()

team_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "soccer_db.team") \
    .option("user", jdbc_props["user"]) \
    .option("password", jdbc_props["password"]) \
    .option("driver", jdbc_props["driver"]) \
    .mode("overwrite") \
    .save()


print("Bronze Success")