# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import sys

from pyspark.sql import *

# ABCDEPress the green button in the gutter to run the script.
from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survey_df, count_by_country

if __name__ == '__main__':
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf = conf) \
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) !=2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    logger.info("Starting HelloSpark")
    # Your processing code
    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())

    survey_df = load_survey_df(spark, sys.argv[1])

    # DF is small and exists in 1 partition. To learning and simulate cloud distributed approach, partitioning df into 2

    partitioned_survey_df = survey_df.repartition(2)

    count_df = count_by_country(partitioned_survey_df)
    logger.info(count_df.collect())

    # For local debugging not for PROD. Doing it to keep app session alive to see UI as it is active during app sesson
    input("Press Enter")
    logger.info("Finished Hello Spark")
    # spark.stop()

















