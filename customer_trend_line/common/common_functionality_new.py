from pyspark.sql import functions as f
from pyspark.sql.functions import *
from pyspark import StorageLevel
from datetime import timedelta
import datetime
import csv
import os.path
from datetime import datetime as d
from common import application_properties
import re
import subprocess as sp

bucket = application_properties.properties['bucket']
back_date = 4
today = datetime.datetime.now().date()
start_date = today - timedelta(days=(today.weekday() - 3) % 7)


def common(passed_df, dc_id_val, dr_id_val, output_path, mara_epcols, epcols2, spark, kernel='PIL', source_kernel='PIL',
           mode="overwrite", mand_col="Yes", is_cplm_quality=False):
    kernel = kernel.upper()
    source_kernel = source_kernel.upper()
    if (kernel != source_kernel):
        mode = 'append'
    df_mapping = spark.read.csv("s3a://" + bucket + "/dqip/BGLogicFile/Prodhier_mapping_UTF8.csv",
                                sep=",", header=True)
    bgi_df1 = passed_df.withColumn("PRDHA_Prefix4", passed_df['PRDHA'].substr(1, 4)).withColumn("PRDHA_Prefix8",
                                                                                                passed_df[
                                                                                                    'PRDHA'].substr(1,
                                                                                                                    8))
    pr8_df = bgi_df1.filter("PRDHA_Prefix4=='9135'")
    pr4_df = bgi_df1.filter((f.col('PRDHA_Prefix4') <> '9135') | (f.col('PRDHA_Prefix4').isNull()))
    df_join1 = pr8_df.join(df_mapping, pr8_df.PRDHA_Prefix8 == df_mapping.Prod_Hier_Prefix, how='left')
    df_join2 = pr4_df.join(df_mapping, pr4_df.PRDHA_Prefix4 == df_mapping.Prod_Hier_Prefix, how='left')
    bgi_df = df_join1.union(df_join2)
    df_output = bgi_df.na.fill({'BusinessGroupID': 'Unallocated'})
    final_df = df_output.withColumn("DC_ID", f.lit(dc_id_val)).withColumn("records", f.lit(1)).withColumn("KernelID",
                                                                                                          f.lit(
                                                                                                              kernel)).withColumn(
        "CompanyCodeId", f.lit("<coco>")).withColumn("Date", f.lit(str(start_date).replace("-", ""))).withColumn(
        "DR_ID", f.lit(dr_id_val)).withColumn("SalesOrganizationID", f.lit("<SORG>")).withColumn("PurchaseOrgID", f.lit(
        "<PORG>")).withColumn("BPCountryID", f.lit("<BCNT>")).withColumn("CCCountryID", f.lit("<CCNT>")).withColumn(
        "SectorID", f.lit("<SECT>"))

    if mand_col == "No":
        epcols1 = [f.col("{}".format(col_name)).alias("{}".format("MARA_" + col_name)) for col_name in mara_epcols]
        epcols_fnl = epcols1 + epcols2
        part1 = dc_id_val.replace("-", "_")
        s3_dir_name = dc_id_val

    else:
        mandtry_cols = ["BISMT", "MSTAE", "PRDHA", "LABOR", "ERSDA", "ERNAM", "LAEDA", "AENAM"]
        epcols3 = ["MAKT_MAKTX", "MARA_MATNR"]
        if 'MATNR' in mara_epcols or 'matnr' in mara_epcols:
            mandtry_cols.append("MATNR")
            r = re.compile("MARA_MATNR")
            newlist = list(filter(r.match, epcols2))
            epcols2 = list(set(epcols2) - set(newlist))
            epcols3 = ["MAKT_MAKTX"]
        epcols_toadd = mara_epcols + mandtry_cols
        epcols_tot = list(set(epcols_toadd))
        epcols1 = [f.col("{}".format(col_name)).alias("{}".format("MARA_" + col_name)) for col_name in epcols_tot]
        epcols_prt = epcols1 + epcols2
        # if source_kernel == "PIL":
        #   epcols3 = ["MAKT_MAKTX","MARA_MATNR"]
        # if source_kernel == "MBP":
        #    epcols3 = ["MAKT_MBP_MAKTX","MARA_MBP_MATNR"]

        epcols = epcols_prt + epcols3
        epcols_fnl = list(set(epcols))
        part1 = dr_id_val.replace("-", "_")
        s3_dir_name = dr_id_val
    #    if source_kernel == 'EUMDR':
    #       part1 = dc_id_val.replace("-", "_")
    mydate = start_date
    part2 = mydate.strftime("%B")
    # part2 = "August"
    part3 = str(mydate.year)
    table_name = part1 + "_" + part2 + "_" + part3
    df3 = final_df.select("Date", "SectorID", "BusinessGroupID", "SalesOrganizationID", "PurchaseOrgID", "BPCountryID",
                          "CCCountryID", "KernelID", "CompanyCodeId", "DC_ID", "DR_ID", "passed", "records",
                          *epcols_fnl)
    write_to_csv_and_hive(output_path, part2, s3_dir_name, table_name, df3, spark, mode, kernel, is_cplm_quality=is_cplm_quality)


def common_noBG(passed_df, dc_id_val, dr_id_val, output_path, epcols, spark, prefix=None, kernel='PIL',
                source_kernel='PIL', mode="overwrite", is_cplm_quality=False):
    kernel = kernel.upper()
    source_kernel = source_kernel.upper()
    if (kernel != source_kernel):
        mode = 'append'
    final_df = passed_df.withColumn("DC_ID", f.lit(dc_id_val)).withColumn("BusinessGroupID",
                                                                          f.lit("Unallocated")).withColumn("records",
                                                                                                           f.lit(
                                                                                                               1)).withColumn(
        "KernelID", f.lit(kernel)).withColumn("CompanyCodeId", f.lit("<coco>")).withColumn("Date", f.lit(
        str(start_date).replace("-", ""))).withColumn("DR_ID", f.lit(dr_id_val)).withColumn("SalesOrganizationID",
                                                                                            f.lit("<SORG>")).withColumn(
        "PurchaseOrgID", f.lit("<PORG>")).withColumn("BPCountryID", f.lit("<BCNT>")).withColumn("CCCountryID", f.lit(
        "<CCNT>")).withColumn("SectorID", f.lit("<SECT>"))
    epcols1 = [f.col("{}".format(col_name)) for col_name in epcols]
    if prefix != None:
        epcols1 = [f.col("{}".format(col_name)).alias("{}".format(prefix + "_" + col_name)) for col_name in epcols]
    df3 = final_df.select("Date", "SectorID", "BusinessGroupID", "SalesOrganizationID", "PurchaseOrgID", "BPCountryID",
                          "CCCountryID", "KernelID", "CompanyCodeId", "DC_ID", "DR_ID", "passed", "records", *epcols1)
    # print("data rule {} is saved").format(dr_id_val)
    part1 = dr_id_val.replace("-", "_")
    mydate = start_date
    part2 = mydate.strftime("%B")
    # part2 = "August"
    part3 = str(start_date.year)
    table_name = part1 + "_" + part2 + "_" + part3
    write_to_csv_and_hive(output_path, part2, dr_id_val, table_name, df3, spark, mode, kernel, is_cplm_quality=is_cplm_quality)


# def common_gts(result_df, dc_id_val, dr_id_val, output_path, spark, prefix=None, kernel='PIL', source_kernel='PIL', mode="overwrite", epcols=None, is_cplm_quality=False):
#     kernel = kernel.upper()
#     source_kernel = source_kernel.upper()
#     if (kernel != source_kernel):
#         mode = 'append'
#     part1 = dr_id_val.replace("-", "_")
#     mydate = start_date
#     part2 = mydate.strftime("%B")
#     part3 = str(start_date.year)
#     table_name = part1 + "_" + part2 + "_" + part3
#     df3 = result_df
#     write_to_csv_and_hive(output_path, part2, dr_id_val, table_name, df3, spark, mode, kernel,is_cplm_quality=is_cplm_quality)

def common_gts(passed_df, dc_id_val, dr_id_val, output_path, epcols, spark, prefix=None, kernel='PIL', source_kernel='PIL', mode="overwrite", is_cplm_quality=False):
    kernel = kernel.upper()
    source_kernel = source_kernel.upper()
    if (kernel != source_kernel):
        mode = 'append'
    df_mapping = spark.read.csv("s3a://" + bucket + "/dqip/BGLogicFile/Prodhier_mapping_UTF8.csv",
                                sep=",", header=True)
    bgi_df1 = passed_df.withColumn("PRDHA_Prefix4", passed_df['PRDHA'].substr(1, 4)).withColumn("PRDHA_Prefix8",
                                                                                                passed_df[
                                                                                                    'PRDHA'].substr(1,
                                                                                                                    8))
    pr8_df = bgi_df1.filter("PRDHA_Prefix4=='9135'")
    pr4_df = bgi_df1.filter((f.col('PRDHA_Prefix4') <> '9135') | (f.col('PRDHA_Prefix4').isNull()))
    df_join1 = pr8_df.join(df_mapping, pr8_df.PRDHA_Prefix8 == df_mapping.Prod_Hier_Prefix, how='left')
    df_join2 = pr4_df.join(df_mapping, pr4_df.PRDHA_Prefix4 == df_mapping.Prod_Hier_Prefix, how='left')
    bgi_df = df_join1.union(df_join2)
    df_output = bgi_df.na.fill({'BusinessGroupID': 'Unallocated'})
    final_df = df_output.withColumn("DC_ID", f.lit(dc_id_val)).withColumn("records", f.lit(1)).withColumn("KernelID",
                                                                                                          f.lit(
                                                                                                              kernel)).withColumn(
        "CompanyCodeId", f.lit("<coco>")).withColumn("Date", f.lit(str(start_date).replace("-", ""))).withColumn(
        "DR_ID", f.lit(dr_id_val)).withColumn("SalesOrganizationID", f.lit("<SORG>")).withColumn("PurchaseOrgID", f.lit(
        "<PORG>")).withColumn("BPCountryID", f.lit("<BCNT>")).withColumn("CCCountryID", f.lit("<CCNT>")).withColumn(
        "SectorID", f.lit("<SECT>"))

    part1 = dr_id_val.replace("-", "_")
    mydate = start_date
    part2 = mydate.strftime("%B")
    part3 = str(mydate.year)
    table_name = part1 + "_" + part2 + "_" + part3 
    #df3 = final_df.drop("PRDHA_Prefix4").drop("PRDHA_Prefix8")
    df3 = final_df.select("Date", "SectorID", "BusinessGroupID", "SalesOrganizationID", "PurchaseOrgID", "BPCountryID",
                          "CCCountryID", "KernelID", "CompanyCodeId", "DC_ID", "DR_ID", "passed", "records",
                          *epcols)
    write_to_csv_and_hive(output_path, part2, dr_id_val, table_name, df3, spark, mode, kernel,is_cplm_quality=is_cplm_quality)

def log_file(df, tname, file_name, kernel, total_from_hive):
    file_exists = os.path.isfile(file_name)
    f = open(file_name, 'a')
    datetime_object = d.now() + timedelta(hours=10, minutes=-30)
    line = tname + "," + kernel + "," + str(df.count()) + "," + str(df.filter("passed==1").count()) + "," + str(
        total_from_hive) + "," + str(datetime_object) + " \n"
    headers = "tablename,kernel,totalcount,passedcount,total_from_hive,time \n"
    if not file_exists:
        writer = f.write(headers)
    f.write(line)
    f.close()


def log_error_file(file_name, msg):
    f = open(file_name, 'a')
    # file_exists = os.path.isfile(file_name)
    datetime_object = d.now() + timedelta(hours=10, minutes=-30)
    line = str(datetime_object) + "|" + msg
    f.write(line)
    f.close()


def write_to_csv_and_hive(output_path, part2, dr_id_val, table_name, df3, spark, mode, kernel, is_cplm_quality=False):
    dr_id_val_temp = ""
    df3 = remove_new_line(df3)
    databases = ["product_rules", "product_rules_cplm_quality"]
    s3_path_substrings = ["/dqip/product/rules/", "/dqip/product/cplm_quality_rules/"]
    database = databases[int(is_cplm_quality)]
    s3_path = s3_path_substrings[int(is_cplm_quality)]
    if (mode == 'append'):
        mode == 'overwrite'
        df3 = sync_columns(output_path, part2, dr_id_val, table_name, df3, spark, s3_path=s3_path)
        df3 = df3.repartition(2)
        dr_id_val_temp = dr_id_val + "_append"
        write_to_csv(output_path, part2, dr_id_val_temp, table_name, df3, spark, mode, kernel)
        # print("csv path","aws s3 mv "+"s3://"+bucket+"/dqip/product/rules/"+part2+"/"+dr_id_val_temp+"/ s3://"+bucket+"/dqip/product/rules/"+part2+"/"+dr_id_val+"/ --recursive")
        # print("parquet path","aws s3 mv "+"s3://"+bucket+"/dqip/product/rules/hive/"+part2+"/"+dr_id_val_temp+"/ s3://"+bucket+"/dqip/product/rules/hive/"+part2+"/"+dr_id_val+"/ --recursive")
        sp.call("aws s3 rm s3://" + bucket + s3_path + part2 + "/" + dr_id_val + "/ --recursive",
                shell=True)
        sp.call(
            "aws s3 mv " + "s3://" + bucket + s3_path + part2 + "/" + dr_id_val_temp + "/ s3://" + bucket + s3_path + part2 + "/" + dr_id_val + "/ --recursive",
            shell=True)
    #         sp.call("aws s3 mv "+"s3://"+bucket+"/dqip/product/rules/hive/"+part2+"/"+dr_id_val_temp+"/ s3://"+bucket+"/dqip/product/rules/hive/"+part2+"/"+dr_id_val+"/ --recursive",shell=True) --TODO
    # df3=spark.read.csv("s3a://"+bucket+"/dqip/product/rules/"+part2+"/"+dr_id_val_temp+"/"+"*.csv",header=True,sep='\t')
    else:
        # print("check---->mode",kernel,mode)
        df3 = df3.repartition(2)
        write_to_csv(output_path, part2, dr_id_val, table_name, df3, spark, mode, kernel)
    #     if(dr_id_val_temp!=""):
    #         #print("SP call","aws s3 rm "+"s3://"+bucket+"/dqip/product/rules/"+part2+"/"+dr_id_val_temp+"/")
    #         sp.call("aws s3 rm "+"s3://"+bucket+"/dqip/product/rules/"+part2+"/"+dr_id_val_temp+"/ --recursive",shell=True)
    #     #for aggregation table. Remove below line in December final run for optimization

    # print("data rule {} is saved").format(dr_id_val)
    spark.sql("drop table if exists "+database+"." + table_name)
    # spark.sql(create_table(df3, output_path + "hive/" + part2 + "/" + dr_id_val, table_name)) --TODO
    spark.sql(create_table(df3, output_path + part2 + "/" + dr_id_val, table_name, db=database))

    # df3.unpersist()
    # log_file(df3,table_name,"countlog.csv",kernel,spark.sql("select count(*) from product_rules."+table_name).collect()[0][0])
    # print(create_table(df3, output_path + part2 + "/" + dr_id_val, table_name))


def write_to_csv(output_path, part2, dr_id_val, table_name, df3, spark, mode, kernel, is_cplm_quality=False):
    df3.write.csv(output_path + "{}/{}".format(part2, dr_id_val), header=True, sep='\t', mode=mode)
    # for each datarule hive table


#     df3.write.parquet(output_path + "hive/" + "{}/{}".format(part2, dr_id_val), mode=mode, compression = "uncompressed") --TODO


def create_table(df, location, tname, db='product_rules'):
    col_types = df.dtypes
    column_string = ",".join(["{} {}".format(col1, type1) for col1, type1 in col_types])
    # print(column_string)
    return "CREATE EXTERNAL TABLE IF NOT EXISTS {0}.{1} ({2})  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' LOCATION '{3}' TBLPROPERTIES('skip.header.line.count'='1')".format(
        db, tname, column_string.replace(" int", " integer"), location)
    # return "CREATE EXTERNAL TABLE IF NOT EXISTS {0}.{1} ({2}) STORED AS PARQUET LOCATION '{3}'".format(db, tname, column_string.replace(" int", " integer"), location) --TODO


def rename_columns(dataframe, prefix):
    columns = dataframe.columns
    columns = [prefix + "_" + col for col in columns]
    dataframe = dataframe.toDF(*columns)
    return dataframe


def remove_new_line(df3):
    df3_cols = df3.columns
    for df3_col in df3_cols:
        df3 = df3.withColumn(df3_col, regexp_replace(col(df3_col), "\n", " "))
    return df3


def sync_columns(output_path, part2, dr_id_val, table_name, df3, spark, s3_path="/dqip/product/rules/"):
    ##print("sync_columns path--> ", output_path + "{}/{}/{}".format(part2, dr_id_val,"*.csv"))
    ##print(output_path,part2,dr_id_val)
    # saved_data=spark.read.csv(output_path + "{}/{}/{}".format(part2, dr_id_val,"*.csv"), header=True, sep='\t')
    # "s3a://"+bucket+"/dqip/product/rules/hive/"+part2+"/"+dr_id_val+"/"+"*.parquet"
    # saved_data = spark.read.parquet("s3a://"+bucket+"/dqip/product/rules/hive/"+part2+"/"+dr_id_val+"/"+"*.parquet") --TODO
    saved_data = spark.read.csv("s3a://" + bucket + s3_path + part2 + "/" + dr_id_val + "/" + "*.csv",
                                header=True, sep='\t')
    df3_cols = df3.columns
    saved_data_cols = saved_data.columns
    for df3_col in df3_cols:
        if (df3_col not in saved_data_cols):
            saved_data = saved_data.withColumn(df3_col, lit("N/A"))
    for saved_data_col in saved_data_cols:
        if (saved_data_col not in df3_cols):
            df3 = df3.withColumn(saved_data_col, lit("N/A"))
    complete_data = df3.union(saved_data)
    return complete_data


def rename_duplicate_columns(dataframe, drop=True):
    # use drop = True when you want to drop the duplicate columns and hold the columns from the table on the left
    # use drop = False when you want to hold the table on both the left side and the right side after performing a join operation
    # when drop is set to False, column names are suffixed with '_P' denoting Primary Table or the table on the left.
    # by default the drop is set to False
    if drop is True:
        columns = dataframe.columns
        duplicate_column_indices = list(set([columns.index(col) for col in columns if columns.count(col) == 2]))
        drop_cols = set()
        for index in duplicate_column_indices:
            drop_cols.add(columns[index])
            columns[index] = columns[index] + '1'
        dataframe = dataframe.toDF(*columns)
        for col in drop_cols:
            dataframe = dataframe.drop(col)
        columns = [col[:-1] if col.endswith("1") else col for col in dataframe.columns]
        dataframe = dataframe.toDF(*columns)
        return dataframe
    else:
        columns = dataframe.columns
        duplicate_column_indices = list(set([columns.index(col) for col in columns if columns.count(col) == 2]))
        for index in duplicate_column_indices:
            columns[index] = columns[index] + "_P"
        dataframe = dataframe.toDF(*columns)
        return dataframe


def clean_column_names(dataframe):
    columns = dataframe.columns
    column_names = list()
    for col in columns:
        if " " in col:
            temp = col.strip()
            column_names.append(temp)
        else:
            column_names.append(col)
    dataframe = dataframe.toDF(*column_names)
    return dataframe