#########################################################################################
# File Name: wkf_s2_mstr_material_ods_stgc_material_uom.py                              #
# Author: Slalom LLC                                                                    #
# Version: 1.0                                                                          #
# Update Date: 05/30/2019                                                               #
#                                                                                       #
# Description: The purpose of this process is to consume the raw extract file from      #
#              the S3 landing zone and perform the ETL process to load the records      #
#              into the parquet file: stgc_material_uom.                                #
#########################################################################################

import sys
import datetime

import pyspark.sql.functions as func
from pyspark.sql.functions import col
from pyspark.sql.types import *

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

        def is_float(str_col):
            try:
                float(str_col)
                return True
            except ValueError:
                return False
            except TypeError:
                return False

        is_float_udf = func.udf(is_float, returnType=BooleanType())

        app_name = 'PySpark - S2 Mstr Material - Stgc'
        env = 'Stage'
        file_name_1 = 'S2_MSTR_MATERIAL.TXT'
        filenames = [file_name_1]
        file_name_list = ",".join(filenames)
        folder_name_1 = 'S2/MSTR_MATERIAL'
        source_type = 'FMS'
        staging_file = 'stgc_material_uom'
        start_time = datetime.datetime.now()
        sys_id = 'S2'
        workflow_name = 'wkf_s2_mstr_material_ods_stgc_material_uom'

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

        # S2_MSTR_MATERIAL flat file
        input_file_1 = "s3://" + ingest_bucket + "/Landing/Received/" + folder_name_1 + "/" + file_name_1
        input_file_1_df = spark.read.text(input_file_1)
        input_1_df = input_file_1_df.select(
            input_file_1_df.value.substr(1, 18).alias("MATNR"),
            input_file_1_df.value.substr(60, 3).alias("MEINS"),
            input_file_1_df.value.substr(63, 13).alias("NTGEW"),
            input_file_1_df.value.substr(76, 3).alias("GEWEI")
        )

        input_1_cnt = input_1_df.count()
        input_1_df = trim_all_cols(input_1_df)
        logger.info("Created df for input file: {} with trimmed and upper cols, df has {} rows"
                    .format(file_name_1, str(input_1_cnt)))

        transformed_df = input_1_df.select(
            col("MATNR").alias("MATERIAL_ID"),
            func.lit(sys_id).cast(StringType()).alias("SYSTEM_ID"),
            col("MEINS").alias("LOCAL_SCE_UOM_ID"),
            func.lit(None).cast(StringType()).alias("SCE_UOM_ID"),
            col("GEWEI").alias("LOCAL_DEST_UOM_ID"),
            func.lit(None).cast(StringType()).alias("DEST_UOM_ID"),
            func.when(is_float_udf(col("NTGEW")), col("NTGEW").cast(DecimalType(30, 15)))
                .otherwise(func.lit(0).cast(DecimalType(30, 15))).alias("EXCHANGE_RATE"),
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
