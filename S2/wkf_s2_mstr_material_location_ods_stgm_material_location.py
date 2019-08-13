#########################################################################################
# File Name: wkf_s2_mstr_material_location_ods_stgm_material_location.py                #
# Author: Slalom LLC                                                                    #
# Version: 1.0                                                                          #
# Update Date: 05/30/2019                                                               #
#                                                                                       #
# Description: The purpose of this process is to consume the raw extract file from      #
#              the S3 landing zone and perform the ETL process to load the records      #
#              into the parquet file: stgm_material_location.                           #
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
        is_float_udf = udf(is_float, returnType=BooleanType())

        app_name = 'PySpark - S2 Mstr Material Location - Stgm'
        env = 'Stage'
        file_name_1 = 'S2_MSTR_MATERIAL_LOCATION_VALUE.TXT'
        file_name_2 = 'S2_MSTR_MATERIAL_LOCATION.TXT'
        file_name_3 = 'S2_MSTR_MATERIAL.TXT'
        filenames = [file_name_1, file_name_2, file_name_3]
        file_name_list = ",".join(filenames)
        folder_name_1 = 'S2/MSTR_MATERIAL_LOCATION_VALUE'
        folder_name_2 = 'S2/MSTR_MATERIAL_LOCATION'
        folder_name_3 = 'S2/MSTR_MATERIAL'
        source_type = 'FMS'
        staging_file = 'stgm_material_location'
        start_time = datetime.datetime.now()
        sys_id = 'S2'
        workflow_name = 'wkf_s2_mstr_material_location_ods_stgm_material_location'

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

        # S2_MSTR_MATERIAL_LOCATION_VALUE flat file
        input_file_1 = "s3://" + ingest_bucket + "/Landing/Received/" + folder_name_1 + "/" + file_name_1
        input_file_1_df = spark.read.text(input_file_1)
        input_1_df = input_file_1_df\
            .select(
                input_file_1_df.value.substr(1, 18).alias("MATNR1"),
                input_file_1_df.value.substr(19, 4).alias("BWKEY"),
                input_file_1_df.value.substr(23, 10).alias("BWTAR"),
                input_file_1_df.value.substr(34, 11).alias("STPRS"),
                input_file_1_df.value.substr(45, 5).alias("PENIH"),
                input_file_1_df.value.substr(50, 4).alias("BKLAS"),
                input_file_1_df.value.substr(54, 5).alias("WAERS")
            )

        input_1_cnt = input_1_df.count()

        logger.info("Created df for input file: {} with trimmed and upper cols, df has {} rows"
                    .format(file_name_1, str(input_1_cnt)))

        # S2_MSTR_MATERIAL_LOCATION flat file
        input_file_2 = "s3://" + ingest_bucket + "/Landing/Received/" + folder_name_2 + "/" + file_name_2
        input_file_2_df = spark.read.text(input_file_2)
        input_2_df = input_file_2_df\
            .select(
                input_file_2_df.value.substr(1, 18).alias("MATNR"),
                input_file_2_df.value.substr(19, 4).alias("WERKS"),
                input_file_2_df.value.substr(51, 1).alias("MAABC"),
            )

        input_2_cnt = input_2_df.count()

        logger.info("Created df for input file: {} with trimmed and upper cols, df has {} rows"
                    .format(file_name_2, str(input_2_cnt)))

        # S2_MSTR_MATERIAL flat file
        input_file_3 = "s3://" + ingest_bucket + "/Landing/Received/" + folder_name_3 + "/" + file_name_3
        input_file_3_df = spark.read.text(input_file_3)
        input_3_df = input_file_3_df\
            .select(
                input_file_3_df.value.substr(1, 18).alias("MATNR2"),
                input_file_3_df.value.substr(28, 4).alias("MTART")
            )

        input_3_cnt = input_3_df.count()

        logger.info("Created df for input file: {} with trimmed and upper cols, df has {} rows"
                    .format(file_name_3, str(input_3_cnt)))

        ####################################################################################
        # Step 2: Join source files and perform final transformations                      #
        ####################################################################################

        # FIL_mstr_material_value
        input_1_df = input_1_df.where("(BWTAR is null) or (length(trim(BWTAR)) == 0)")

        # FIL_local_location_id
        input_2_df = input_2_df.where("WERKS not in ('BP01', 'BP02')")

        # JNR_material_location
        joined_df = input_2_df.join(input_1_df,
                                    (input_2_df["MATNR"] == input_1_df["MATNR1"]) &
                                    (input_2_df["WERKS"] == input_1_df["BWKEY"]),
                                    "left_outer")

        # JNR_mstr_material_with_location
        joined_2_df = joined_df.join(input_3_df,
                                     joined_df["MATNR"] == input_3_df["MATNR2"],
                                     "left_outer")\
                               .withColumn("v_STPRS", func.when(is_float_udf(col("STPRS")), col("STPRS").cast(DoubleType()))
                                           .otherwise(func.lit(0).cast(DoubleType())))\
                               .withColumn("v_PENIH", func.when(is_float_udf(col("PENIH")), col("PENIH").cast(DoubleType()))
                                           .otherwise(func.lit(0).cast(DoubleType())))

        transformed_df = joined_2_df.select(
            func.trim(func.upper(col("MATNR"))).alias("MATERIAL_ID"),
            func.lit(sys_id).cast(StringType()).alias("SYSTEM_ID"),
            func.trim(func.upper(col("WERKS"))).alias("LOCAL_LOCATION_ID"),
            func.lit(None).cast(StringType()).alias("LOCATION_ID"),
            func.when((col("v_STPRS") > 0) & (col("v_PENIH") > 0), (col("v_STPRS") / col("v_PENIH")).cast(DecimalType(30, 15)))
                .otherwise(func.lit(0).cast(DecimalType(30, 15))).alias("STD_COST"),
            func.when((col("STPRS").isNull()) | (func.length(func.trim(col("STPRS"))) == 0), "BRL")
                .otherwise(func.trim(func.upper(col("WAERS")))).alias("LOCAL_CURRENCY_ID"),
            func.lit(None).cast(StringType()).alias("CURRENCY_ID"),
            func.lit(None).cast(StringType()).alias("LOCAL_PROFIT_CENTER_ID"),
            func.lit(None).cast(StringType()).alias("PROFIT_CENTER_ID"),
            func.when(((col("BKLAS").isNull()) | (func.length(func.trim(col("BKLAS"))) == 0)) & ((col("MTART").isNull()) | (func.length(func.trim(col("MTART"))) == 0)), "EXPN")
                .otherwise(func.when((col("BKLAS").isNull()) | (func.length(func.trim(col("BKLAS"))) == 0), func.trim(func.upper(col("MTART")))).otherwise(func.trim(func.upper(col("BKLAS")))))
                .alias("LOCAL_INVENTORY_TYPE"),
            func.lit(None).cast(StringType()).alias("INVENTORY_TYPE"),
            func.upper(col("MAABC")).alias("MATERIAL_CLASS"),
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

        logger.info("Successfully wrote transformed df and input file")

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
