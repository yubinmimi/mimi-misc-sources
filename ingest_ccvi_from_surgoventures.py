# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

path = "/Volumes/mimi_ws_1/surgoventures/src/ccvi.xlsx"

# COMMAND ----------

pdf = pd.read_excel(path, sheet_name=0, dtype={'FIPS': str})
pdf.columns = change_header(pdf.columns)

# COMMAND ----------

pdf['fips'] = pdf['fips'].str.pad(width=2, fillchar='0')
pdf['mimi_src_file_date'] = parse('2021-01-01').date()
pdf['mimi_src_file_name'] = 'ccvi.xlsx'
pdf['mimi_dlt_load_date'] = datetime.today().date()

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE mimi_ws_1.surgoventures.ccvi_county;

# COMMAND ----------

spark.createDataFrame(pdf).write.mode('overwrite').saveAsTable('mimi_ws_1.surgoventures.ccvi_state')

# COMMAND ----------

pdf = pd.read_excel(path, sheet_name=1, dtype={'FIPS': str})
pdf.columns = change_header(pdf.columns)

# COMMAND ----------

pdf['fips'] = pdf['fips'].str.pad(width=5, fillchar='0')
pdf['mimi_src_file_date'] = parse('2021-01-01').date()
pdf['mimi_src_file_name'] = 'ccvi.xlsx'
pdf['mimi_dlt_load_date'] = datetime.today().date()

# COMMAND ----------

spark.createDataFrame(pdf).write.mode('overwrite').saveAsTable('mimi_ws_1.surgoventures.ccvi_county')
