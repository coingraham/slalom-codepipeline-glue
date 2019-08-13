#########################################################################################
# File Name: wkf_s2_mstr_customer_ods_stgm_customer.py                                  #
# Author: Slalom LLC                                                                    #
# Version: 1.0                                                                          #
# Update Date: 05/30/2019                                                               #
#                                                                                       #
# Description: The purpose of this process is to consume the raw extract file from      #
#              the S3 landing zone and perform the ETL process to load the records      #
#              into the parquet file: stgm_customer.                                    #
#########################################################################################

import sys
import datetime

import pyspark.sql.functions as func
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.window import Window

from wrk_ppm_general import *


def main():
    try:
        ################################################################################
        # This area will have customized values specific for each ETL process.         #
        ################################################################################

        # Creating function for only trimming columns (no upper or remove_non_print)
        def trim_all_cols(df):
            for col in df.columns:
                df = df.withColumn(col, func.ltrim(func.rtrim(df[col])))
            return df

        app_name = 'PySpark - S2 Mstr Customer - Stgm'
        env = 'Stage'
        file_name_1 = 'S2_MSTR_CUSTOMER_KNB1.TXT'
        file_name_2 = 'S2_MSTR_CUSTOMER.TXT'
        filenames = [file_name_1, file_name_2]
        file_name_list = ",".join(filenames)
        folder_name_1 = 'S2/MSTR_CUSTOMER_KNB1'
        folder_name_2 = 'S2/MSTR_CUSTOMER'
        source_type = 'FMS'
        staging_file = 'stgm_customer'
        start_time = datetime.datetime.now()
        sys_id = 'S2'
        workflow_name = 'wkf_s2_mstr_customer_ods_stgm_customer'

        # Check if S3 bucket names were passed to script
        if len(sys.argv) != 4:
            raise ValueError
        else:
            datalake_bucket = sys.argv[1]  # DbucketName = 'wrk-datalake-ppm-dev'
            ingest_bucket = sys.argv[2]  # IbucketName = 'wrk-ingest-ppm-dev'
            system_bucket = sys.argv[3]  # SbucketName = 'wrk-system-ppm-dev'

        spark = create_spark_session(app_name)
        spark.sparkContext.setLogLevel('INFO')

        logger = create_logger(__name__, spark)

        ####################################################################################
        # Step 1:                                                                          #
        #  - Initialize lookup procedures
        #  - Read the input files                                                          #
        #  - Select required fields                                                        #
        #  - Apply rtrim, ltrim, and upper functions to all cols                           #
        ####################################################################################

        """ Source Definitions """

        # S2_MSTR_CUSTOMER_KNB1 flat file
        input_file_1 = "s3://" + ingest_bucket + "/Landing/Received/" + folder_name_1 + "/" + file_name_1
        input_file_1_df = spark.read.text(input_file_1)
        input_1_df = input_file_1_df\
            .select(
                input_file_1_df.value.substr(4, 10).alias("KUNNR1"),
                input_file_1_df.value.substr(14, 4).alias("BUKRS"),
                input_file_1_df.value.substr(92, 10).alias("ZWELS"),
                input_file_1_df.value.substr(104, 4).alias("ZTERM")
            )

        input_1_cnt = input_1_df.count()
        input_1_df = trim_all_cols(input_1_df)

        logger.info("Created df for input file: {} with trimmed and upper cols, df has {} rows"
                    .format(file_name_1, str(input_1_cnt)))

        # S2_MSTR_CUSTOMER flat file
        input_file_2 = "s3://" + ingest_bucket + "/Landing/Received/" + folder_name_2 + "/" + file_name_2
        input_file_2_df = spark.read.text(input_file_2)
        input_2_df = input_file_2_df\
            .select(
                func.ltrim(func.rtrim(input_file_2_df.value.substr(4, 10))).alias("KUNNR"),
                input_file_2_df.value.substr(14, 3).alias("LAND1"),
                input_file_2_df.value.substr(17, 35).alias("NAME1"),
                input_file_2_df.value.substr(87, 35).alias("ORT01"),
                input_file_2_df.value.substr(122, 10).alias("PSTLZ"),
                input_file_2_df.value.substr(132, 3).alias("REGIO"),
                input_file_2_df.value.substr(145, 35).alias("STRAS"),
                input_file_2_df.value.substr(180, 16).alias("TELF1"),
                input_file_2_df.value.substr(196, 31).alias("TELFX"),
                input_file_2_df.value.substr(415, 18).alias("ERDAT"),
                input_file_2_df.value.substr(460, 10).alias("KONZS"),
                input_file_2_df.value.substr(470, 4).alias("KTOKD"),
                input_file_2_df.value.substr(571, 35).alias("ORT02")
            )

        input_2_cnt = input_2_df.count()

        logger.info("Created df for input file: {} with trimmed and upper cols, df has {} rows"
                    .format(file_name_2, str(input_2_cnt)))

        ####################################################################################
        # Step 2: Join source files and perform final transformations                      #
        ####################################################################################

        # AGG_mstr_customer_sales
        agg_window = Window().partitionBy(input_1_df.KUNNR1).rowsBetween(-sys.maxsize, sys.maxsize)
        input_1_df = input_1_df.withColumn("max_BUKRS", func.max(col("BUKRS")).over(agg_window))

        # JNRTRANS
        joined_df = input_2_df.join(input_1_df,
                                    input_2_df["KUNNR"] == input_1_df["KUNNR1"],
                                    "left_outer")

        transformed_df = joined_df.select(
            func.upper(func.ltrim(func.rtrim(col("KUNNR")))).alias("CUSTOMER_ID"),
            func.lit(sys_id).alias("SYSTEM_ID"),
            func.upper(func.ltrim(func.rtrim(col("NAME1")))).alias("CUSTOMER_DESC"),
            func.when(func.trim(col("KONZS")) == "01795995", "I")
                .otherwise(func.trim(func.upper(col("KTOKD")))).alias("LOCAL_CUSTOMER_TYPE"),
            func.lit(None).cast(StringType()).alias("CUSTOMER_TYPE"),
            func.upper(func.ltrim(func.rtrim(col("ZTERM")))).alias("LOCAL_PAYMENT_TERMS_ID"),
            func.lit(None).cast(StringType()).alias("PAYMENT_TERMS_ID"),
            func.upper(func.ltrim(func.rtrim(col("ZWELS")))).alias("LOCAL_PAYMENT_METHOD_ID"),
            func.lit(None).cast(StringType()).alias("PAYMENT_METHOD_ID"),
            func.lit(None).cast(StringType()).alias("DB_NBR"),
            func.lit(None).cast(StringType()).alias("SALES_REP"),
            func.date_format(col("ERDAT"), "yyyymmdd").cast(TimestampType()).alias("FIRST_SHIP_DATE"),
            func.upper(func.ltrim(func.rtrim(col("STRAS")))).alias("ADDRESS_1"),
            func.upper(func.ltrim(func.rtrim(col("ORT02")))).alias("ADDRESS_2"),
            func.lit(None).cast(StringType()).alias("ADDRESS_3"),
            func.upper(func.ltrim(func.rtrim(col("ORT01")))).alias("CITY"),
            func.lit(None).cast(StringType()).alias("COUNTY"),
            func.upper(func.ltrim(func.rtrim(col("REGIO")))).alias("LOCAL_STATE_ID"),
            func.lit(None).cast(StringType()).alias("STATE_ID"),
            func.upper(func.ltrim(func.rtrim(col("LAND1")))).alias("LOCAL_COUNTRY_ID"),
            func.lit(None).cast(StringType()).alias("COUNTRY_ID"),
            func.upper(func.ltrim(func.rtrim(col("PSTLZ")))).alias("POSTAL_CODE"),
            func.upper(func.ltrim(func.rtrim(col("TELF1")))).alias("TELEPHONE_1"),
            func.upper(func.ltrim(func.rtrim(col("TELFX")))).alias("TELEPHONE_2"),
            func.lit(source_type).cast(StringType()).alias("SOURCE_TYPE"),
            func.lit(format_current_dt()).cast(TimestampType()).alias("DATESTAMP"),
            func.lit(None).cast(StringType()).alias("PROCESSING_FLAG")
        )

        ####################################################################################
        # Step 3: Write transformed dataframe to staging                                   #
        ####################################################################################
        target_schema = spark.sql("select * from ppmods.{}".format(staging_file)).schema
        staging_s3_path = "s3://" + datalake_bucket + "/Transformed/" + staging_file + "/"
        if validate_schema(transformed_df.schema, target_schema):
            transformed_df.write.parquet(staging_s3_path, mode='append', compression='snappy')
        else:
            transformed_df.printSchema()
            raise ValueError("Transformed df does not match schema of target table: {}".format(staging_file))

        logger.info("Successfully wrote transformed df and input file with count: {}".format(str(transformed_df.count())))

        ####################################################################################
        # Step 4. Write out the log details.                                               #
        ####################################################################################
        input_file_cnt_list = str(input_1_cnt) + ',' + str(input_2_cnt)
        write_log_file(workflow_name, sys_id, file_name_list, input_file_cnt_list, env, start_time, '', 'Success',
                       system_bucket, spark)
        system_cleanup(spark)

    except ValueError as value_error:
        logger.error(value_error)
        system_cleanup(spark)
    except Exception as e:
        # Explicitly passing blank value for input_file_cnt_list to avoid exception if file counts are not initialized
        # input_file_cnt_list = str(input_file_cnt_1)
        write_log_file(workflow_name, sys_id, file_name_list, '', env, start_time, e, 'Error',
                       system_bucket, spark)
        logger.error(e)
        system_cleanup(spark)


if __name__ == "__main__":
    main()
