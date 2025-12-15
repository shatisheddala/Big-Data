from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, last, to_date
from pyspark.sql.window import Window

spark = (SparkSession.builder
         .appName("Soccer Silver Layer")
         .enableHiveSupport()
         .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")


# Hadoop Server
#namenode = "ip-172-31-3-80.eu-west-2.compute.internal:9000"

#bronze_base = "hdfs://" + namenode + "/UKUS18nov/Joshua/soccer-project/bronze/"
#silver_base = "hdfs://" + namenode + "/UKUS18nov/Joshua/soccer-project/silver/"


bronze_base = "/user/ec2-user/UKUS18nov/Joshua/soccer-project/bronze/"
silver_base = "/user/ec2-user/UKUS18nov/Joshua/soccer-project/silver/"



# Read Bronze data
country_df = spark.read.parquet(bronze_base + "country")
league_df = spark.read.parquet(bronze_base + "league")
team_df = spark.read.parquet(bronze_base + "team")
team_attributes_df = spark.read.parquet(bronze_base + "team_attributes")
player_df = spark.read.parquet(bronze_base + "player")
player_attributes_df = spark.read.parquet(bronze_base + "player_attributes")
match_df = spark.read.parquet(bronze_base + "match")


## Data Cleaning

## Data Cleaning
#print(country_df.columns)
#print(league_df.columns)
#print(team_df.columns)
#print(team_attributes_df.columns)
#print(player_df.columns)
#print(player_attributes_df.columns)
#print(match_df.columns)

"""
['id', 'name']
['id', 'country_id', 'name']
['id', 'team_api_id', 'team_fifa_api_id', 'team_long_name', 'team_short_name']
['id', 'team_fifa_api_id', 'team_api_id', 'date', 
'buildUpPlaySpeed', 'buildUpPlaySpeedClass', 'buildUpPlayDribbling', 'buildUpPlayDribblingClass', 
'buildUpPlayPassing', 'buildUpPlayPassingClass', 'buildUpPlayPositioningClass', 'chanceCreationPassing', 
'chanceCreationPassingClass', 'chanceCreationCrossing', 'chanceCreationCrossingClass', 
'chanceCreationShooting', 'chanceCreationShootingClass', 'chanceCreationPositioningClass', 
'defencePressure', 'defencePressureClass', 'defenceAggression', 'defenceAggressionClass', 
'defenceTeamWidth', 'defenceTeamWidthClass', 'defenceDefenderLineClass']
['id', 'player_api_id', 'player_name', 'player_fifa_api_id', 'birthday', 'height', 'weight']
['id', 'player_fifa_api_id', 'player_api_id', 'date', 'overall_rating', 'potential', 'preferred_foot', 
'attacking_work_rate', 'defensive_work_rate', 'crossing', 'finishing', 'heading_accuracy', 
'short_passing', 'volleys', 'dribbling', 'curve', 'free_kick_accuracy', 'long_passing', 'ball_control', 
'acceleration', 'sprint_speed', 'agility', 'reactions', 'balance', 'shot_power', 'jumping', 'stamina', 
'strength', 'long_shots', 'aggression', 'interceptions', 'positioning', 'vision', 'penalties', 
'marking', 'standing_tackle', 'sliding_tackle', 'gk_diving', 'gk_handling', 'gk_kicking', 
'gk_positioning', 'gk_reflexes']
['id', 'country_id', 'league_id', 'season', 'stage', 'date', 'match_api_id', 
'home_team_api_id', 'away_team_api_id', 'home_team_goal', 'away_team_goal', 
'home_player_X1', 'home_player_X2', 'home_player_X3', 'home_player_X4', 'home_player_X5', 
'home_player_X6', 'home_player_X7', 'home_player_X8', 'home_player_X9', 'home_player_X10', 
'home_player_X11', 
'away_player_X1', 'away_player_X2', 'away_player_X3', 'away_player_X4', 'away_player_X5', 
'away_player_X6', 'away_player_X7', 'away_player_X8', 'away_player_X9', 'away_player_X10', 
'away_player_X11', 
'home_player_Y1', 'home_player_Y2', 'home_player_Y3', 'home_player_Y4', 'home_player_Y5', 
'home_player_Y6', 'home_player_Y7', 'home_player_Y8', 'home_player_Y9', 'home_player_Y10', 
'home_player_Y11', 'away_player_Y1', 'away_player_Y2', 'away_player_Y3', 'away_player_Y4', 
'away_player_Y5', 'away_player_Y6', 'away_player_Y7', 'away_player_Y8', 'away_player_Y9', 
'away_player_Y10', 'away_player_Y11', 
'home_player_1', 'home_player_2', 'home_player_3', 'home_player_4', 'home_player_5', 'home_player_6', 
'home_player_7', 'home_player_8', 'home_player_9', 'home_player_10', 'home_player_11', 
'away_player_1', 'away_player_2', 'away_player_3', 'away_player_4', 'away_player_5', 'away_player_6', 
'away_player_7', 'away_player_8', 'away_player_9', 'away_player_10', 'away_player_11', 
'goal', 'shoton', 'shotoff', 'foulcommit', 'card', 'cross', 'corner', 'possession', 'B365H', 'B365D', 
'B365A', 'BWH', 'BWD', 'BWA', 'IWH', 'IWD', 'IWA', 'LBH', 'LBD', 'LBA', 'PSH', 'PSD', 'PSA', 'WHH', 
'WHD', 'WHA', 'SJH', 'SJD', 'SJA', 'VCH', 'VCD', 'VCA', 'GBH', 'GBD', 'GBA', 'BSH', 'BSD', 'BSA']
"""


# Country
"""
No Nulls
"""
country_silver = (country_df
                  .withColumnRenamed("id", "country_id")
                  .withColumnRenamed("name", "country_name")
                  )


# League
"""
Noticed that the country_id column has the exact same values as id
Doing a sanity check between the league names and the countries showed this was correct
Decided country table was not needed, as League encompassed it after joining to add Country name
No Nulls
"""
league_silver = (league_df
                 .withColumnRenamed("id", "league_id")
                 .withColumnRenamed("name", "league_name")
                 .join(country_silver, league_df['country_id'] == country_silver['country_id'], how='right')
                 .select(['league_id', league_df['country_id'], 'country_name', 'league_name'])
)


# Team
"""
There were 3 different ids: 'id', 'team_api_id', 'team_fifa_api_id' -> first and third unique,
 but 'team_api_id' had 3 duplicates corresponding to the same 'team_long_name', but different 
 'team_short_name'
Researched and concluded incorrect join in original data source and dropped the incorrect names, 
then used just team_api_id, renamed to team_id as sole id for table
No Nulls
"""
team_silver = (
    team_df
    .filter(~col('team_short_name').isin(['LOD', 'GOR', 'MOP']))
    .withColumnRenamed("team_api_id", "team_id")
    .select(['team_id', 'team_long_name', 'team_short_name'])
    )


# Team_Attributes
"""
As with team, drop fifa id
In the data set, all datetime stamps don't have time, so we just reformat to a date
buildUpPlayDribbling is the only column with nulls, and about 10% is null, so we 
    decided to drop it as well.  We also note that buildUpPlayDribblingClass has 
    value 'Little' wherever it is null, so the dataset seems to be classifying nulls near 0.
Date was in Epoch time, so changed format and renamed col
"""

#team_attributes_df.select("date").show(5, False)

team_attributes_silver = (
    team_attributes_df
    .drop('team_fifa_api_id', 'buildUpPlayDribbling')
    .withColumnRenamed("team_api_id", "team_id")
    .withColumnRenamed("id", "team_att_id")
    .withColumn("date", to_date(from_unixtime(col("date") / 1000)))
    .withColumnRenamed("date", "team_measured_date")
    
)


# Player
"""
As with Team, there are 3 different ids, this time all unique 
 We drop 'id' and 'player_fifa_api_id', but keep 'player_api_id'
All datetime stamps don't have time, so we just reformat to a date
No nulls
Date was in Epoch time, so changed format
"""
player_silver = (player_df
                 .drop('id', 'player_fifa_api_id')
                 .withColumnRenamed("player_api_id", "player_id")
                 .withColumn("birthday", to_date(from_unixtime(col("birthday") / 1000)))
)


# Player_Attributes
"""
As with Team, there are 3 ids, we drop 'player_fifa_api_id', but keep 'player_api_id'
This time we must also keep 'id', because the same player is measured multiple times at different dates 
All datetime stamps don't have time and in epoch time, so we just reformat to a date, and 
rename to measured_date

There are Nulls in all except for 'player_id', 'player_att_id', 'measured_date'
 Since the same player is measured more than once, we fill the nulls by 
 1. Carrying the last known rating forward.
 2. Backfilling if early rows are null
 For now keep rest of Nulls as is -> Deal with later in Gold Layer
"""
player_attributes_silver = (player_attributes_df
                            .drop('player_fifa_api_id')
                            .withColumnRenamed("id", "player_att_id")
                            .withColumnRenamed("player_api_id", "player_id")
                            .withColumn("date",to_date(from_unixtime(col("date") / 1000)))
                            .withColumnRenamed("date", "player_measured_date")
)

pa_rating_cols = [
    c for c in player_attributes_silver.columns if c not in ['player_id', 'player_att_id', 'player_measured_date']]

window = Window.partitionBy("player_id").orderBy("player_measured_date")
for c in pa_rating_cols:
    player_attributes_silver = player_attributes_silver.withColumn(
        c,
        last(col(c), ignorenulls=True).over(window)
    )

window_rev = Window.partitionBy("player_id").orderBy(col("player_measured_date").desc())
for c in pa_rating_cols:
    player_attributes_silver = player_attributes_silver.withColumn(
        c,
        last(col(c), ignorenulls=True).over(window_rev)
    )


rows_with_nulls = player_attributes_silver.filter(
    " OR ".join([c + " IS NULL" for c in pa_rating_cols])
)

#rows_with_nulls.show(truncate=False)


# Match
"""
Match has a lot of data that isn't particular useful, so we drop all the positional data columns about 
the players and all the betting columns.

Match also has a lot of Nulls that we will deal with in the Gold Layer
"""
match_cols = ['match_api_id', 'country_id', 'league_id', 'season', 'stage', 'date', 'home_team_api_id', 'away_team_api_id', 'home_team_goal', 'away_team_goal', 'home_player_1', 'home_player_2', 'home_player_3', 'home_player_4', 'home_player_5', 'home_player_6', 'home_player_7', 'home_player_8', 'home_player_9', 'home_player_10', 'home_player_11', 'away_player_1', 'away_player_2', 'away_player_3', 'away_player_4', 'away_player_5', 'away_player_6', 'away_player_7', 'away_player_8', 'away_player_9', 'away_player_10', 'away_player_11']

match_silver = match_df.select(match_cols)


# Write Silver Data 

# Create Hive database
spark.sql("""
    CREATE DATABASE IF NOT EXISTS soccer_silver
    LOCATION '/user/ec2-user/UKUS18nov/Joshua/soccer-project/silver-hive/soccer_silver.db'
""")


# don't write country, not needed
league_silver.write.mode("overwrite").saveAsTable("soccer_silver.league")
team_silver.write.mode("overwrite").saveAsTable("soccer_silver.team")
team_attributes_silver.write.mode("overwrite").saveAsTable("soccer_silver.team_attributes")
player_silver.write.mode("overwrite").saveAsTable("soccer_silver.player")
player_attributes_silver.write.mode("overwrite").saveAsTable("soccer_silver.player_attributes")
match_silver.write.mode("overwrite").saveAsTable("soccer_silver.match")

print("Silver Layer Complete")


