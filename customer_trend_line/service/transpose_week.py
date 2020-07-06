from pyspark.sql.functions import *
from pyspark.sql.functions import when, lit, trim, upper
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from common.common_functionality import common, common_noBG, rename_columns, rename_duplicate_columns,adding_record_column
import datetime
from pyspark.sql.types import StringType,IntegerType,FloatType
from pyspark.sql import DataFrame
from dateutil.relativedelta import *
from datetime import timedelta
import pandas as pd
import subprocess as sp

date=datetime.datetime.now()


properties= {"bucket":"s3-dqip-data-np","customer_data":"data/"}

 

bucket = properties['bucket']
customer_data_raw = properties['customer_data']



source_path = "s3a://"+bucket+"/qlik_business_layer_data/data_model_data/raw_data/"


output_file="F_KPI_Processed"
file_name="F_KPI_finance_table_raw.txt"
file_name="F_KPI_table_raw.txt"
output_file="F_KPI_Processed_multi_domain"
##########BulkLoad############
file_name="F_KPI_full_table_raw.txt"
output_file="F_KPI_Processed_bulk_load"
##############################

f_kpi_fin= spark.read.csv(source_path +file_name, sep="\t", header=True)
cc_country=spark.read.csv(source_path + "CC_Country_table_raw.csv", sep=",", header=True)

source_path = "s3a://"+bucket+"/qlik_business_layer_data/data_model_data/processed_data/"
date_raw_df = spark.read.csv(source_path + "date_table.csv", sep=",", header=True)
j_dat=date_raw_df.join(f_kpi_fin,f_kpi_fin['`%Date.KEY`']==date_raw_df.Key,'inner')
defect_col_list=["week","CompanyCodeID_F","SalesOrg","BP_Country","ID","passed","records","FLAGID"]
j_dat=j_dat.withColumnRenamed("%BusinessGroup.KEY","BusinessGroup").withColumnRenamed("%SalesOrg.KEY","SalesOrg").withColumnRenamed("%CC_Country.KEY","CCCountryID").withColumnRenamed("%BP_Country.KEY","BP_Country").withColumn("passed",f.col("# Passed").cast(IntegerType())).withColumn("records",f.col("# Records").cast(IntegerType())).select(defect_col_list)
mandate_cols=["CompanyCodeID_F","SalesOrg","BP_Country","ID","FLAGID"]
j_dat_passed1=j_dat.withColumn("week",f.concat(f.lit("week_"),f.col("week"),f.lit("_passed")))
j_dat_records1=j_dat.withColumn("week",f.concat(f.lit("week_"),f.col("week"),f.lit("_records")))
j_data_passed=j_dat_passed1.groupBy(mandate_cols).pivot("week").sum("passed")
j_data_records=j_dat_records1.groupBy(mandate_cols).pivot("week").sum("records")
date=datetime.datetime.now()
condition=[]
for col in mandate_cols:
    j_data_records=j_data_records.withColumnRenamed(col,col+"_records")
    condition.append(f.col(col)==f.col(col+"_records"))


bulk_join=j_data_passed.join(j_data_records,condition,"inner")
for col in mandate_cols:
    bulk_join=bulk_join.drop(col+"_records")
    


bulk_join.repartition(f.col("FLAGID")).write.partitionBy("FLAGID").csv(source_path+output_file, header=True, sep='\t', mode="overwrite")
date1=datetime.datetime.now()
print(str(date1-date))
j_data_passed.repartition(f.col("FLAGID")).write.partitionBy("FLAGID").csv(source_path+output_file, header=True, sep='\t', mode="overwrite")
j_data_records.repartition(f.col("FLAGID")).write.partitionBy("FLAGID").csv(source_path+output_file, header=True, sep='\t', mode="overwrite")
#j_dat1=j_dat.withColumn("week",f.concat(f.lit("week_"),f.col("week"),f.lit("_passed"))) # week addition in 


#############################################################1 week process ######################################
week_int=int(j_dat.select("week").take(1)[0][0])
week=str(week_int)+"_week"
save_data=j_dat.groupBy(mandate_cols).agg(f.sum("passed").alias(week+"_passed"),f.sum("records").alias(week+"_records"))
for i in range(week_int+1,week_int+6):
    multi=(i-week_int)*week_int
    week=str(i)+"_week"
    save_data=save_data.withColumn(week+"_passed",f.col(str(week_int)+"_week_passed")*multi).withColumn(week+"_records",f.col(str(week_int)+"_week_records")*multi)


save_data.repartition(f.col("FLAGID")).write.partitionBy("FLAGID").csv(source_path+output_file, header=True, sep='\t', mode="overwrite") # we need bucketed by the madatory columns  
date1=datetime.datetime.now()
print(str(date1-date))

#####################################################################################################################






#Note
#Comapany code, CC_country_table join with fkoi [source group,source group],ID,SalesOrg,BP_Country,S_DR




#Total_piviot_time= 0:25:10.751085


#Total_time after joining data and persistance : - 0:27:57.266475

#  total time on multikernel with many columns as mandatory:- 0:31:26.565468   # count :- 151459490   One Hundred Fifty One Million Four Hundred Fifty Nine Thousand Four Hundred Ninety





##################################################################

from pyspark.sql.functions import *
from pyspark.sql.functions import when, lit, trim, upper
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from common.common_functionality import common, common_noBG, rename_columns, rename_duplicate_columns,adding_record_column
import datetime
from pyspark.sql.types import StringType,IntegerType,FloatType
from pyspark.sql import DataFrame
from dateutil.relativedelta import *
from datetime import timedelta
import pandas as pd
import subprocess as sp

date=datetime.datetime.now()


properties= {"bucket":"s3-dqip-data-np","customer_data":"data/"}

 

bucket = properties['bucket']
customer_data_raw = properties['customer_data']

 

source_path = "s3a://"+bucket+"/qlik_business_layer_data/data_model_data/raw_data/"


output_file="F_KPI_Processed"
file_name="F_KPI_finance_table_raw.txt"

##########BulkLoad############
file_name="F_KPI_full_table_raw.txt"
output_file="F_KPI_Processed_bulk_load"
##############################
file_name="F_KPI_table_raw.txt"
output_file="F_KPI_Processed_multi_domain"

f_kpi_fin= spark.read.csv(source_path +file_name, sep="\t", header=True)
cc_country=spark.read.csv(source_path + "CC_Country_table_raw.csv", sep=",", header=True)

source_path = "s3a://"+bucket+"/qlik_business_layer_data/data_model_data/processed_data/"
date_raw_df = spark.read.csv(source_path + "date_table.csv", sep=",", header=True)
j_dat=date_raw_df.join(f_kpi_fin,f_kpi_fin['`%Date.KEY`']==date_raw_df.Key,'inner')
defect_col_list=["week","CompanyCodeID_F","SalesOrg","BP_Country","ID","passed","records","FLAGID"]

col_list=["`"+col+"`" if "%" in col else col for col in f_kpi_fin.columns]
col_list.remove("`%Date.KEY`")
defect_col_list=col_list+["week"]
#j_dat=j_dat.withColumnRenamed("%BusinessGroup.KEY","BusinessGroup").withColumnRenamed("%SalesOrg.KEY","SalesOrg").withColumnRenamed("%CC_Country.KEY","CCCountryID").withColumnRenamed("%BP_Country.KEY","BP_Country").withColumn("passed",f.col("# Passed").cast(IntegerType())).withColumn("records",f.col("# Records").cast(IntegerType())).select(defect_col_list)

j_dat=j_dat.withColumn("# Passed",f.col("# Passed").cast(IntegerType())).withColumn("# Records",f.col("# Records").cast(IntegerType())).select(defect_col_list)
mandate_cols=["CompanyCodeID_F","SalesOrg","BP_Country","ID","FLAGID"]
mandate_cols=col_list

#############################################################1 week process ######################################
week_int=int(j_dat.select("week").take(1)[0][0])
week=str(week_int)+"_week"
save_data=j_dat.groupBy(mandate_cols).agg(f.sum("# Passed").alias(week+"_passed"),f.sum("# Records").alias(week+"_records"))
for i in range(week_int+1,week_int+6):
    multi=(i-week_int)*week_int
    week=str(i)+"_week"
    save_data=save_data.withColumn(week+"_passed",f.col(str(week_int)+"_week_passed")*multi).withColumn(week+"_records",f.col(str(week_int)+"_week_records")*multi)


save_data.repartition(f.col("FLAGID")).write.partitionBy("FLAGID").csv(source_path+output_file, header=True, sep='\t', mode="overwrite") # we need bucketed by the madatory columns  
date1=datetime.datetime.now()
print(str(date1-date))

#####################################################################################################################

j_dat_passed1=j_dat.withColumn("week",f.concat(f.lit("week_"),f.col("week"),f.lit("_passed")))
j_dat_records1=j_dat.withColumn("week",f.concat(f.lit("week_"),f.col("week"),f.lit("_records")))
j_data_passed=j_dat_passed1.groupBy(mandate_cols).pivot("week").sum("passed")
j_data_records=j_dat_records1.groupBy(mandate_cols).pivot("week").sum("records")
date=datetime.datetime.now()
condition=[]
for col in mandate_cols:
    j_data_records=j_data_records.withColumnRenamed(col,col+"_records")
    condition.append(f.col(col)==f.col(col+"_records"))


bulk_join=j_data_passed.join(j_data_records,condition,"inner")
for col in mandate_cols:
    bulk_join=bulk_join.drop(col+"_records")
    


bulk_join.repartition(f.col("FLAGID")).write.partitionBy("FLAGID").csv(source_path+output_file, header=True, sep='\t', mode="overwrite")
date1=datetime.datetime.now()
print(str(date1-date))
j_data_passed.repartition(f.col("FLAGID")).write.partitionBy("FLAGID").csv(source_path+output_file, header=True, sep='\t', mode="overwrite")
j_data_records.repartition(f.col("FLAGID")).write.partitionBy("FLAGID").csv(source_path+output_file, header=True, sep='\t', mode="overwrite")
#j_dat1=j_dat.withColumn("week",f.concat(f.lit("week_"),f.col("week"),f.lit("_passed"))) # week addition in 

