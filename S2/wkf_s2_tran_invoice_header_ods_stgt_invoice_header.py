#########################################################################################
# File Name: wkf_s2_tran_invoice_header_ods_stgt_invoice_header.py                      #
# Author: Slalom LLC                                                                    #
# Version: 1.0                                                                          #
# Update Date: 05/30/2019                                                               #
#                                                                                       #
# Description: The purpose of this process is to consume the raw extract file from      #
#              the S3 landing zone and perform the ETL process to load the records      #
#              into the parquet file: stgt_invoice_header.                                #
#########################################################################################

import sys
import datetime

import pyspark.sql.functions as func
from pyspark.sql.functions import col, udf
from pyspark.sql.types import *

from wrk_ppm_general import *


def main():
    try:
        ################################################################################
        # This area will have customized values specific for each ETL process.         #
        ################################################################################

        def is_date(date_str):
            try:
                formatted_date = datetime.datetime.strptime(date_str, '%Y%m%d').strftime('%Y%m%d')
            except:
                formatted_date = datetime.datetime.strptime('19000101', '%Y%m%d').strftime('%Y%m%d')
            return formatted_date

        is_date_udf = udf(is_date, returnType=StringType())
        app_name = 'PySpark - S2 Tran Invoice Header - Stgt'
        env = 'Stage'
        file_name_1 = 'S2_TRAN_INVOICE_HEADER.TXT'
        filenames = [file_name_1]
        file_name_list = ",".join(filenames)
        folder_name_1 = 'S2/TRAN_INVOICE_HEADER'
        source_type = 'FMS'
        staging_file = 'stgt_invoice_header'
        start_time = datetime.datetime.now()
        sys_id = 'S2'
        workflow_name = 'wkf_s2_tran_invoice_header_ods_stgt_invoice_header'

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
        #  - Read the input files                                                          #
        #  - Select required fields                                                        #
        #  - Apply rtrim, ltrim, and upper functions to all cols                           #
        ####################################################################################

        """ Source Definitions """

        # S2_TRAN_INVOICE_HEADER flat file
        input_file_1 = "s3://" + ingest_bucket + "/Landing/Received/" + folder_name_1 + "/" + file_name_1
        input_file_1_df = spark.read.text(input_file_1)
        input_1_df = input_file_1_df\
            .select(
                input_file_1_df.value.substr(1, 10).alias("VBELN"),
                func.trim(func.upper(input_file_1_df.value.substr(11, 4))).alias("FKART"),
                input_file_1_df.value.substr(17, 5).alias("WAERK"),
                input_file_1_df.value.substr(22, 4).alias("VKORG"),
                input_file_1_df.value.substr(46, 8).alias("FKDAT"),
                input_file_1_df.value.substr(117, 14).alias("KURRF"),
                input_file_1_df.value.substr(175, 17).alias("NETWR"),
                input_file_1_df.value.substr(274, 10).alias("KUNAG"),
                input_file_1_df.value.substr(698, 10).alias("KUNWE")
            )

        input_1_cnt = input_1_df.count()
        logger.info("Created df for input file: {} with trimmed and upper cols, df has {} rows"
                    .format(file_name_1, str(input_1_cnt)))

        input_1_df = input_1_df.where("FKART != 'ZFD3'")

        transformed_df = input_1_df.select(
            func.trim(func.upper(col("VBELN"))).alias("LOCAL_INVOICE_ID"),
            func.lit(sys_id).cast(StringType()).alias("SYSTEM_ID"),
            func.trim(func.upper(col("KUNAG"))).alias("SOLD_CUSTOMER_ID"),
            func.trim(func.upper(col("FKART"))).alias("LOCAL_INVOICE_TYPE"),
            func.lit(None).cast(StringType()).alias("INVOICE_TYPE"),
            func.trim(func.upper(col("VKORG"))).alias("LOCAL_BUSINESS_UNIT_ID"),
            func.lit(None).cast(StringType()).alias("BUSINESS_UNIT_ID"),
            func.when((func.trim(func.upper(func.substring(col("FKART"), 1, 2))) == 'S1') |
                      (func.trim(func.upper(func.substring(col("FKART"), 1, 2))).isin('ZG2B','ZREB','ZROB')),
                      (col("NETWR").cast(DoubleType()) * -1 * col("KURRF").cast(DoubleType())).cast(DecimalType(20, 4)))
                .otherwise((col("NETWR").cast(DoubleType()) * col("KURRF").cast(DoubleType())).cast(DecimalType(20, 4)))
                .alias("INVOICE_VALUE"),
            func.lit("BRL").cast(StringType()).alias("LOCAL_CURRENCY_ID"),
            func.lit(None).cast(StringType()).alias("CURRENCY_ID"),
            is_date_udf(col("FKDAT")).cast(TimestampType()).alias("INVOICE_DATE"),
            is_date_udf(col("FKDAT")).cast(TimestampType()).alias("POST_DATE"),
            func.lit(source_type).cast(StringType()).alias("SOURCE_TYPE"),
            func.lit(format_current_dt()).cast(TimestampType()).alias("DATESTAMP"),
            func.lit(None).cast(StringType()).alias("PROCESSING_FLAG")
        )

        ####################################################################################
        # Step 2: Write transformed dataframe to staging                                   #
        ####################################################################################
        target_schema = spark.sql("select * from ppmods.{}".format(staging_file)).schema
        staging_s3_path = "s3://" + datalake_bucket + "/Transformed/" + staging_file + "/"
        if validate_schema(transformed_df.schema, target_schema):
            transformed_df.write.parquet(staging_s3_path, mode='append', compression='snappy')
        else:
            raise ValueError("Transformed df does not match schema of target table: {}".format(staging_file))

        logger.info("Successfully wrote transformed df and input file")

        ####################################################################################
        # Step 4. Write out the log details.                                               #
        ####################################################################################
        input_file_cnt_list = ",".join([str(input_1_cnt)])
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
