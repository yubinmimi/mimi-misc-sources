# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

import json
path = "/Volumes/mimi_ws_1/mimilabs/src/cmsemails"

# COMMAND ----------

data = []
for filepath in Path(path).glob('*.json'):
    doc = json.load(open(filepath, "r"))
    mimi_src_file_date = parse(doc['date'][:10]).date()
    mimi_src_file_name = filepath.name
    mimi_dlt_load_date = datetime.today().date()
    row = [doc['subject'], 
           parse(doc['date']), 
           doc['sender_name'], 
           doc['sender_email'],
           doc['content'],
           mimi_src_file_date,
           mimi_src_file_name,
           mimi_dlt_load_date]
    data.append(row)

# COMMAND ----------

pdf = pd.DataFrame(data, columns=['subject', 'date', 'sender_name', 'sender_email', 'content', 'mimi_src_file_date', 'mimi_src_file_name', 'mimi_dlt_load_date'])
df = spark.createDataFrame(pdf)
df.write.mode("overwrite").saveAsTable("mimi_ws_1.mimilabs.cmsemails")

# COMMAND ----------


