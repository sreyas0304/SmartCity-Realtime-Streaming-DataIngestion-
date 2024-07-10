from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
from config import configuration
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
import socket

def check_kafka_connectivity(broker, port):
    try:
        with socket.create_connection((broker, port), timeout=5):
            print("Kafka broker is reachable")
    except Exception as e:
        print(f"Error connecting to Kafka broker: {e}")



def main():

    # Get AWS credentials from configuration
    aws_access_key = configuration.get('AWS_ACCESS_KEY')
    aws_secret_key = configuration.get('AWS_SECRET_KEY')

    if not aws_access_key or not aws_secret_key:
        raise ValueError("AWS_ACCESS_KEY or AWS_SECRET_KEY is not set in the configuration.")

    # Print out the configuration for debugging
    print("AWS Access Key:", aws_access_key)
    print("AWS Secret Key:", aws_secret_key)
    
    # Define the dependencies
    dependencies = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.apache.kafka:kafka-clients:2.6.0",
        "org.apache.hadoop:hadoop-aws:3.3.1",
        "com.amazonaws:aws-java-sdk:1.11.469"
    ]
    
    # Join dependencies into a comma-separated string
    dependencies_str = ",".join(dependencies)
    print("Dependencies:", dependencies_str)
    
    spark = SparkSession.builder.appName("SmartCityStreaming")\
        .config("spark.jars.packages", dependencies_str)\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key)\
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)\
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
        .getOrCreate()

    # spark = SparkSession.builder.appName("SmartCityStreaming")\
    # .config("spark.jars.packages",
    #         "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0",
    #         "org.apache.hadoop:hadoop-aws:3.3.1",
    #         "com.amazonaws:aws-java-sdk:1.11.469")\
    # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    # .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY'))\
    # .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY'))\
    # .config("spark.hadoop.fs.s3a.aws.credentials.provider", 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
    # .getOrCreate()

    # adjust the log level to minimize the console
    spark.sparkContext.setLogLevel('WARN')

    #vehicle schema
    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True)
    ])

    #gps schema
    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True)
    ])

    #traffic schema
    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("camera_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True),
    ])

    #weather schema
    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("AQI", DoubleType(), True)
    ])

    #emergency schema
    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("incident_id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True)
    ])

    def read_kafka_topic(topic, schema):
        try:
            check_kafka_connectivity('broker', 29092)
            df = (spark.readStream
                  .format('kafka')
                  .option('kafka.bootstrap.servers', 'broker:29092')
                  .option('subscribe', topic)
                  .option('startingOffsets', 'earliest')
                  .load()
                  .selectExpr('CAST(value AS STRING)')
                  .select(from_json(col('value'), schema).alias('data'))
                  .select('data.*')
                  .withWatermark('timestamp', '2 minutes'))
            return df
        except Exception as e:
            print(f"Error reading Kafka topic {topic}: {e}")
            raise
        # return (spark.readStream
        #         .format('kafka')
        #         .option('kafka.bootstrap.servers', 'broker:29092')
        #         .option('subscribe', topic)
        #         .option('startingoffsets', 'earliest')
        #         .load()
        #         .selectExpr('CAST(value AS STRING')
        #         .select(from_json(col('value'), schema).alias('data'))
        #         .select('data.*')
        #         .withWatermark('timestamp', '2 minutes')
        #         )
    
    def streamWriter(input:DataFrame, checkpointFolder, output):
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start())
    
    vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficDF = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', emergencySchema).alias('emergency')

    # join all the dfs using id and timestamp


    query1 = streamWriter(vehicleDF, 's3a://smartcity-streaming-data/checkpoints/vehicle_data', 
                 's3a://smartcity-streaming-data/data/vehicle_data')
    query2 = streamWriter(gpsDF, 's3a://smartcity-streaming-data/checkpoints/gps_data', 
                 's3a://smartcity-streaming-data/data/gps_data')
    query3 = streamWriter(trafficDF, 's3a://smartcity-streaming-data/checkpoints/traffic_data', 
                 's3a://smartcity-streaming-data/data/traffic_data')
    query4 = streamWriter(weatherDF, 's3a://smartcity-streaming-data/checkpoints/weather_data', 
                 's3a://smartcity-streaming-data/data/weather_data')
    query5 = streamWriter(emergencyDF, 's3a://smartcity-streaming-data/checkpoints/emergency_data', 
                 's3a://smartcity-streaming-data/data/emergency_data')
    
    query5.awaitTermination()

    


if __name__ == "__main__":
    main()