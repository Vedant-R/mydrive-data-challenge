########################################## Libraries ##################################################
# Importing required dependencies
import glob
import sys, os
import random
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from matplotlib import pyplot as plt
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import summary_table

########################################## Random Sampling #############################################
########################################## Question(1) #################################################
# Collecting driver directory names in a list
driver_directories = glob.glob(sys.argv[1] + "*")
#driver_directories = glob.glob("C:/myDrive/data/interview-data/trips/*")

# Random Sampling based on number of driver directories in a list
random_sample = [driver_directories[i] for i in sorted(random.sample(range(len(driver_directories)), 112))]

# Collecting processed json files from random sampled driver directories in a list
processed_json_files = []
for i in random_sample:
   processed_json_files.append(glob.glob(i + "/*processed.json.gz"))

# Collecting processed json files for all drivers in a list
flat_processed_json_list = [item for sublist in processed_json_files for item in sublist]

########################################## Spark #######################################################
# Creating Spark Config and Context
conf = SparkConf().setAppName("myDrive")
sc = SparkContext(conf = conf)

# Configuring Spark Session
spark = SparkSession(sc)

# Retrieving trips file name in a spark dataframe
spark_jsons_df = spark.read.json(flat_processed_json_list).withColumn("trip_id", F.input_file_name())

# Exploding data column
spark_explode_df = spark_jsons_df.withColumn("data", F.explode(F.col("data")))
spark_df = spark_explode_df.select(
        "data.distance",
        "data.sub_id",
        "data.vehicle",
        "data.link_id",
			"data.time",
			"data.lat",
			"data.lng",
			"data.speed",
			"trip_id")

########################################## Function Definitions ########################################
########################################## Question(2) #################################################
# Function to find total distance in km and total time in seconds of all trips
def q2(df, distance, speed):
    
    q2_df = df\
            .select(distance, speed)\
            .groupBy()\
            .agg(F.sum(F.col(distance)).alias("sum_of_" + distance), 
                 F.sum(F.col(speed)).alias("sum_of_" + speed))\
            .withColumn("sum_of_" + distance, F.col("sum_of_" + distance) * 1000)\
            .withColumn("total_time", F.col("sum_of_" + distance) / F.col("sum_of_" + speed))\
            .withColumn("total_time", F.col("total_time") * 3600)\
            .drop("sum_of_" + speed)
            
    return q2_df

########################################## Question (3)(b) #############################################
# Function to find total distance in km, average speed in kph and total time in hours per driver
def q3_driver(df, driver_id, distance, speed):
    
    q3_driver_df = df\
                    .select(driver_id, distance, speed)\
                    .groupBy(driver_id)\
                    .agg(F.sum(F.col(distance)).alias("sum_of_" + distance), 
                         F.avg(F.col(speed)).alias("avg_of_" + speed), 
                         F.sum(F.col(speed)).alias("sum_of_" + speed))\
                    .withColumn("sum_of_" + distance, F.col("sum_of_" + distance) * 1000)\
                    .withColumn("total_time", F.col("sum_of_" + distance) / F.col("sum_of_" + speed))\
                    .drop("sum_of_speed")
    
    return q3_driver_df

########################################## Question(3)(a) #############################################
# Function to find total distance in km, average speed in kph and total time in hours per trip, 
# (based on processed json file name)
def q3_trip(df, trip_id, distance, speed):    
    
    q3_trip_df = df\
                .select(trip_id, distance, speed)\
                .groupBy(trip_id)\
                .agg(F.sum(F.col(distance)).alias("sum_of_" + distance), 
                       F.avg(F.col(speed)).alias("avg_of_" + speed), 
                       F.sum(F.col(speed)).alias("sum_of_" + speed))\
                .withColumn("sum_of_" + distance, F.col("sum_of_" + distance) * 1000)\
                .withColumn("total_time", F.col("sum_of_" + distance) / F.col("sum_of_" + speed))\
                .drop("sum_of_speed")
    
    return q3_trip_df

########################################## Visualisations #############################################
########################################## Question(5) ################################################
# Function to generate scatter plots
def scatterplot(df,column1,column2, file_name_distinguish):
    _, ax = plt.subplots()
    ax.scatter(df[column1], df[column2], s=60, color="#539caf", alpha=0.75)
    ax.set_title(column1 + "  vs  " + column2)
    ax.set_xlabel(column1)
    ax.set_ylabel(column2)
    plt.savefig(os.getcwd() + "/" + (column1 + "_vs_" + column2) + "_" + file_name_distinguish + "_scattered.png")

# Function for linear regression
def linearR(df, column1, column2):
    
    x = sm.add_constant(df.toPandas()[column1])
    y = df.toPandas()[column2]
    regr = sm.OLS(y, x)
    res = regr.fit()
    st, data, ss2 = summary_table(res, alpha=0.05)
    fitted_values = data[:,2]
    return fitted_values
    
# Function for generating line plot
def lineplot(df, column1, column2, fitted_values, file_name_distinguish):
    
    _, ax = plt.subplots()
    ax.plot(df.toPandas()[column1], fitted_values, lw = 2, color = "#539caf", alpha = 1)
    ax.set_title(column1 + "  vs  " + column2)
    ax.set_xlabel(column1)
    ax.set_ylabel(column2)
    plt.savefig(os.getcwd() + "/" + (column1 + "_vs_" + column2) + "_" + file_name_distinguish + "_linear.png")

########################################## Function Calls #############################################
# Calling Function to find total distance in km and total time in seconds of all trips
sum_time_df = q2(spark_df, "distance", "speed")
sum_time_df.show()

# Function to find total distance in km, average speed in kph and total time in hours per driver
each_driver_df = q3_driver(spark_df, "sub_id", "distance", "speed")
each_driver_df.show()

# Calling Function to find total distance in km, average speed in kph and total time in hours per trip 
each_trip_df = q3_trip(spark_df, "trip_id", "distance", "speed")
each_trip_df.show()

######################################### Plots for Question(3)(b) ####################################
# Generating scatter plot for Distance Travelled vs Average speed Per Driver
scatterplot(each_driver_df.toPandas(), "sum_of_distance", "avg_of_speed", "driver")

# Generating scatter plot for Distance Travelled vs Total Time Per Driver
scatterplot(each_driver_df.toPandas(), "sum_of_distance", "total_time", "driver")

# Generating scatter plot for Average Speed vs Total Time Per Driver
scatterplot(each_driver_df.toPandas(), "avg_of_speed", "total_time", "driver")

######################################### Plots for Question(3)(a) ####################################
# Generating scatter plot for Distance Travelled vs Average speed Per Trip
scatterplot(each_trip_df.toPandas(), "sum_of_distance", "avg_of_speed", "trip")

# Generating scatter plot for Distance Travelled vs Total Time Per Trip
scatterplot(each_trip_df.toPandas(), "sum_of_distance", "total_time", "trip")

# Generating scatter plot for Average Speed vs Total Time Per Trip
scatterplot(each_trip_df.toPandas(), "avg_of_speed", "total_time", "trip")

######################################### Linear Regression for (3)(b) ################################
# Calling Linear Regression Model function and Plotting Total Distance vs Average Speed Per Driver
lineplot(each_driver_df,
         "sum_of_distance",
         "avg_of_speed", 
         linearR(each_driver_df, "sum_of_distance", "avg_of_speed"),
         "driver")

# Calling Linear Regression Model function and Plotting Total Distance vs Average Speed Per Driver
lineplot(each_driver_df,
         "sum_of_distance",
         "total_time", 
         linearR(each_driver_df, "sum_of_distance", "total_time"),
         "driver")

# Calling Linear Regression Model function and Plotting Average Speed vs Total Time Per Driver
lineplot(each_driver_df,
         "avg_of_speed",
         "total_time", 
         linearR(each_driver_df, "avg_of_speed", "total_time"),
         "driver")

######################################### Linear Regression for (3)(a) ################################
# Calling Linear Regression Model function and Plotting Total Distance vs Average Speed Per Trip
lineplot(each_trip_df,
         "sum_of_distance",
         "avg_of_speed", 
         linearR(each_trip_df, "sum_of_distance", "avg_of_speed"),
         "trip")

# Calling Linear Regression Model function and Plotting Total Distance vs Average Speed Per Trip
lineplot(each_trip_df,
         "sum_of_distance",
         "total_time", 
         linearR(each_trip_df, "sum_of_distance", "total_time"),
         "trip")

# Calling Linear Regression Model function and Plotting Average Speed vs Total Time Per Trip
lineplot(each_trip_df,
         "avg_of_speed",
         "total_time", 
         linearR(each_trip_df, "avg_of_speed", "total_time"),
         "trip")