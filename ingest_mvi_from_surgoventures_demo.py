# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS mimi_ws_1.surgoventures.mvi_county (
# MAGIC   county_fips STRING,
# MAGIC   county_name STRING,
# MAGIC   mvi DOUBLE,
# MAGIC   other_variables STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE mimi_ws_1.surgoventures.mvi_county IS '# The [U.S. Maternal Vulnerability Index](https://mvi.surgoventures.org/) by [Surgo Ventures](https://surgoventures.org/)
# MAGIC
# MAGIC Surgo Ventures, a non-profit organization dedicated to solving health and social problems with precision, developed the U.S. Maternal Vulnerability Index (MVI) to measure the health and social wellbeing of mothers in the United States. For more details about the MVI data and methodology, please visit the [MVI website](https://mvi.surgoventures.org/).
# MAGIC
# MAGIC ## Prerequisites for accessing the data
# MAGIC
# MAGIC To access/use the data, please contact [mvi@surgohealth.com](mailto:mvi%40surgohealth.com) with the following template:
# MAGIC
# MAGIC > Title: [MVI access request from mimilabs] - <your name and organization>
# MAGIC
# MAGIC > Dear the MVI team at Surgo Ventures,
# MAGIC
# MAGIC > <Please provide your name/organization, and brief background>
# MAGIC > <Please provide 3-4 sentences on your intended research purpose or use cases>
# MAGIC
# MAGIC After the receipt of the email, Surgo Ventures will contact you for the remaining paperwork. Once the approval is granted, the data will be made available for your account.
# MAGIC
# MAGIC ## Data Methods and Limitations (excerpts from the Surgo Ventures website)
# MAGIC
# MAGIC The MVI covers 50 U.S. states and the District of Columbia. The index is a relative metric that ranks the vulnerability of U.S. geographic units against each other and is not an absolute measure of vulnerability. Because the index relies on datasets available for the whole U.S., it may not include all the factors associated with adverse maternal health. In addition, the index might not capture state-specific programs that seek to improve maternal health and/or disparities in outcomes. Though the influence of individual themes on vulnerability may vary by geography, a single weighting scheme was used to calculate the index across the U.S. The geographic levels used to calculate and map the index might mask more granular inequities in exposure to unfavorable environments for maternal outcomes. For more details, please writes to [mvi@surgoventures.org](mailto:mvi%40surgoventures.org). Finally, robust data on birth outcomes as they pertain to the birthing individuals are limited and are generally based on information collected for purposes other than public health surveillance.
# MAGIC ';

# COMMAND ----------


