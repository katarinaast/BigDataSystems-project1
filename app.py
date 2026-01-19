import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, avg, stddev

SPARK_URL = "spark://spark-master:7077"
HDFS_PATH = "hdfs://namenode:9000/data/crimes_1gb.csv"


def filter_count(spark, year, crime_type):

    start = time.time()

    df = spark.read.option("header", "true").csv(HDFS_PATH, inferSchema=True)
    df = df.withColumn("Year", col("Year").cast("int"))

    result = df.filter(
        (col("Year") == year) &
        (col("Primary Type") == crime_type)
    )

    print(f"\nBroj incidenata ({crime_type}) u {year}. godini: {result.count()}")
    result.show(5)

    return time.time() - start

def stats_by(spark, group_col, numeric_col):

    start = time.time()

    df = spark.read.option("header", "true").csv(HDFS_PATH, inferSchema=True)
    df = df.withColumn(numeric_col, col(numeric_col).cast("double"))

    stats = (
        df.groupBy(group_col)
        .agg(
            min(col(numeric_col)).alias("MIN"),
            max(col(numeric_col)).alias("MAX"),
            avg(col(numeric_col)).alias("AVG"),
            stddev(col(numeric_col)).alias("STDDEV")
        )
    )

    stats.show(50)

    return time.time() - start

if __name__ == "__main__":

    args = sys.argv
    print(args)

    if len(args) < 4:
        print("Usage:")
        print(" spark-submit crime_app.py <APP> <MODE> <TASK> args...")
        sys.exit(1)

    app_name = "CrimeApp/" + args[1]
    mode = args[2]  # local or spark
    task = args[3]

    spark_master = "local[2]" if mode == "local" else SPARK_URL
    start_app = time.time()
    spark = SparkSession.builder.appName(app_name).master(spark_master).getOrCreate()
    task_time = 0

    if task == "filter-count":
        if len(args) < 6:
            print("Usage: filter-count <YEAR> <TYPE>")
            sys.exit(1)

        year = int(args[4])
        crime = args[5]

        print("\nRunning FILTER COUNT...")
        task_time = filter_count(spark, year, crime)

    elif task == "stats-by":
        if len(args) < 6:
            print("Usage: stats-by <GROUP_COL> <NUM_COL>")
            sys.exit(1)

        group_col = args[4]
        num_col = args[5]

        print("\nRunning STATISTICS...")
        task_time = stats_by(spark, group_col, num_col)

    else:
        print("Unknown task:", task)
        sys.exit(1)

    print(f"\nVreme izvrsenja taska: {round(task_time, 3)} sec")
    print(f"Vreme cele aplikacije: {round(time.time() - start_app, 3)} sec")

    spark.stop()
    sys.exit()
