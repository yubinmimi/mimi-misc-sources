# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS mimi_ws_1.nasbo.state_expenditure;

# COMMAND ----------

pdf_key = pd.read_csv("/Volumes/mimi_ws_1/nasbo/src/1991-2023 State Expenditure Report Data-key.csv").dropna(how='all')

# COMMAND ----------

dtypes = {row['COLUMN HEADING']: float for _, row in pdf_key.iterrows()}
dtypes["YEAR"] = int
dtypes["STATE"] = str

# COMMAND ----------

pdf = pd.read_csv("/Volumes/mimi_ws_1/nasbo/src/1991-2023 State Expenditure Report Data.csv",
                  dtype=dtypes, 
                  thousands = ',')

# COMMAND ----------

pdf.columns = change_header(pdf.columns)

# COMMAND ----------

# All years reported are state fiscal years unless otherwise indicated. In 46 states the fiscal year begins on July 1 and ends on June 30. The exceptions are as follows: in New York the fiscal year begins on April 1; in Texas, the fiscal year begins on September 1; and in Alabama and Michigan the fiscal year begins on October 1. Additionally, the length of budget cycles vary among states, with more than half of the states budgeting annually and the remainder enacting biennial budgets.
# We use July 30
pdf['fiscal_year_end_date'] = pdf['year'].apply(lambda x: parse(str(int(x))+'-06-30').date())

# COMMAND ----------


pdf["mimi_src_file_date"] = parse('2023-12-31').date()
pdf['mimi_src_file_name'] = "1991-2023 State Expenditure Report Data.csv"
pdf['mimi_dlt_load_date'] = datetime.today().date()

# COMMAND ----------

spark.createDataFrame(pdf).write.mode("overwrite").saveAsTable("mimi_ws_1.nasbo.state_expenditure")

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON SCHEMA mimi_ws_1.nasbo IS '# Datasets from the [National Association of State Budget Officers](https://www.nasbo.org/home).
# MAGIC ';

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE mimi_ws_1.nasbo.state_expenditure IS '# State Expenditure Report data ($ in Millions).
# MAGIC
# MAGIC ## Methodology
# MAGIC
# MAGIC Expenditure data are detailed by program area and funding source so that trends in state spending can be evaluated. Users of the data are cautioned that a more complete understanding of service levels within a given state would require comparisons of spending by both state and local governments, which is not the purpose of this report. In addition, the data are self-reported by the states. States were also asked to include footnotes where there are unusual state budget practices, exceptions, or where a figure is unobtainable. Footnotes for each State Expenditure Report can be found in the indiviudal yearly reports: https://www.nasbo.org/reports-data/state-expenditure-report/state-expenditure-archives. 
# MAGIC
# MAGIC This report documents state expenditures in six functional categories: elementary and secondary education, higher education, public assistance including Temporary Assistance for Needy Families and other cash assistance, Medicaid, corrections, and transportation. All other expenditures make up a seventh category. The report includes expenditures from four fund sources, including general funds, federal funds, other state funds, and bonds. Data for each category typically include employer contributions to current employees’ pensions and to employee health benefits for employees. Dollar amounts in the dataset are reported in millions.
# MAGIC Elementary and secondary education spending, detailed in chapter one, includes state and federal fund expenditures only, and excludes local funds raised for education purposes. States also were asked to include, where applicable, state expenditures that support the state’s Department of Education, early education/pre-K, capital construction, transportation of school children, adult literacy programs, handicapped education programs, programs for other special populations (i.e., gifted and talented programs), anti-drug programs, and vocational education. States were asked to exclude spending for day care programs in the school system, spending for school health and immunization programs, and local funds raised and expended for education purposes. 
# MAGIC For higher education, states were requested to include expenditures made for capital construction, community colleges, vocational education, law, medical, veterinary, nursing and technical schools, and assistance to private colleges and universities, as well as tuition, fees and student loan programs. Higher education expenditures exclude federal research grants and endowments to universities. Higher education data can be found in chapter two.
# MAGIC
# MAGIC Spending for public assistance, which is examined in chapter three, includes expenditures for the Temporary Assistance for Needy Families (TANF) program and other cash assistance (i.e., state supplements to the Supplemental Security Income program, general or emergency assistance). States were asked to exclude administrative costs from reported expenditures.  
# MAGIC
# MAGIC Medicaid spending amounts, highlighted in chapter four, exclude administrative costs, but include spending from state funds, federal matching funds and other funds and revenue sources used as a Medicaid match such as provider taxes, fees, assessments, donations, and local funds. Medicare Part D clawback payments are included in a state’s overall Medicaid expenditures.
# MAGIC
# MAGIC For corrections, states were asked to include, where applicable, expenditures for capital construction, aid to local governments for jails, parole programs, prison industries, community corrections, drug abuse rehabilitation programs, as well as expenditures made for juvenile correction programs. Corrections data can be found in chapter five.
# MAGIC
# MAGIC Transportation figures, detailed in chapter six, include capital and operating expenditures for highways, mass transit, and airports. States were also asked to include expenditures for road assistance to local governments, the administration of the Department of Transportation, truck and train/railroad programs, motor vehicle licensing, and gas tax and fee collection. The data exclude spending for port authorities, state police and highway patrol. States were also asked to separately detail transportation fund revenue sources. 
# MAGIC
# MAGIC The “all other” expenditure category includes all remaining programs not captured in the functional categories previously described, including the Children’s Health Insurance Program and any debt service for other state programs (i.e., environmental projects, housing). States with lotteries were asked to exclude prizes paid to lottery winners.  States were also asked to exclude expenditures for state-owned utilities and liquor stores. “All other” expenditure data can be found in chapter seven. States were also asked to separately detail debt service spending. 
# MAGIC
# MAGIC Capital spending is included with operating expenditures within each functional category, unless otherwise noted. Capital expenditures have also been collected separately in the following categories:  elementary and secondary education, higher education, transportation, corrections, housing, environmental, and “all other.” Capital expenditure data can be found in Chapter Eight.
# MAGIC
# MAGIC All years reported are state fiscal years unless otherwise indicated. In 46 states the fiscal year begins on July 1 and ends on June 30. The exceptions are as follows: in New York the fiscal year begins on April 1; in Texas, the fiscal year begins on September 1; and in Alabama and Michigan the fiscal year begins on October 1. Additionally, the length of budget cycles vary among states, with more than half of the states budgeting annually and the remainder enacting biennial budgets.
# MAGIC
# MAGIC Beginning in the 2023 State Expenditure Report, data were included from multiple U.S. territories (Guam, Puerto Rico, and the U.S. Virgin Islands) and the District of Columbia. Puerto Rico begins its fiscal year on July 1, while the District of Columbia, Guam, and the U.S. Virgin Islands begin their fiscal year on October 1. 
# MAGIC
# MAGIC ## Definitions
# MAGIC
# MAGIC - General Fund: predominant fund for financing a state’s operations. Revenues are received from broad-based state taxes. There are differences in how specific functions are financed from state to state, however.  
# MAGIC - Federal Funds: funds received directly from the federal government.
# MAGIC - Other State Funds: expenditures from revenue sources, which are restricted by law for particular governmental functions or activities. For example, a gasoline tax dedicated to a highway trust fund would appear in the “Other State Funds” column (Note:  For Medicaid, other state funds include provider taxes, fees, donations, assessments and local funds).
# MAGIC - Bonds: expenditures from the sale of bonds, generally for capital projects.
# MAGIC - State Funds: general fund plus other state fund spending, excluding state spending from bonds.
# MAGIC ';

# COMMAND ----------

for _, row in pdf_key.iterrows():
    varname = format_header_varname(row["COLUMN HEADING"])
    desc = row['Unnamed: 1']
    stmt = f"ALTER TABLE mimi_ws_1.nasbo.state_expenditure ALTER COLUMN {varname} COMMENT '{desc}';"
    spark.sql(stmt)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revenue File

# COMMAND ----------

pdf = pd.read_excel("/Volumes/mimi_ws_1/nasbo/src/NASBO_GF_Revenue_Historical_Dataset_1997-2023.xlsx",
                  thousands = ',',
                  sheet_name = 'GF Revenue Data ($ in millions)')
pdf.columns = change_header(pdf.columns)
pdf['fiscal_year_end_date'] = pdf['fiscal_year'].apply(lambda x: parse(str(int(x))+'-06-30').date())
pdf["mimi_src_file_date"] = parse('2023-12-31').date()
pdf['mimi_src_file_name'] = "NASBO_GF_Revenue_Historical_Dataset_1997-2023.xlsx"
pdf['mimi_dlt_load_date'] = datetime.today().date()
spark.createDataFrame(pdf).write.mode("overwrite").saveAsTable("mimi_ws_1.nasbo.general_fund_revenue")

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE mimi_ws_1.nasbo.general_fund_revenue IS '# State General Fund Revenue data ($ in Millions).
# MAGIC
# MAGIC ## Data Source
# MAGIC
# MAGIC Survey data were self-reported by executive state budget offices. For fiscal 1997-2021, data are from NASBO\'s State Expenditure Report, available at  https://www.nasbo.org/reports-data/state-expenditure-report. For fiscal 2022 and beyond, data are from National Association of State Budget Officers (NASBO), Fall Fiscal Survey of States, available at https://www.nasbo.org/mainsite/reports-data/fiscal-survey-of-states. [Beginning in 2023, general fund revenue data were no longer collected in the State Expenditure Report.] 
# MAGIC
# MAGIC ## Data Notes
# MAGIC
# MAGIC Data represent $ in millions and are not adjusted for inflation. Data reported in this dataset represent actuals.
# MAGIC
# MAGIC For fiscal 1997-2021, data are provided for the 50 states. Beginning in fiscal 2022, data are also included for the District of Columbia and three U.S. territories (Guam, Puerto Rico, and the Virgin Islands).
# MAGIC
# MAGIC Through 2022, NASBO collected general fund revenue figures broken down by five categories: Sales Tax; Personal Income Tax; Corporate Income Tax; Gaming Revenue; and All Other Revenue. Beginning in 2023, NASBO eliminated Gaming Revenue as a separate category and that revenue is now reported in the All Other category. Accordingly, this dataset has consolidated historical data to include gaming revenue in the All Other category. 
# MAGIC
# MAGIC For most states, general fund revenue data reported in the State Expenditure Report through fiscal 2021 is comparable to reported figures in the Fall Fiscal Survey beginning in fiscal 2022. However, for select states, discrepancies may exist for a variety of reasons. These differences usually relate to variations in how revenue is categorized between each revenue type, differing exclusions of certain revenue streams/transfers from general fund figures in the two surveys, and whether tax refunds are accounted for within each tax category or in the all other category. Therefore, data users should exercise caution in making year-over-year comparisons, especially from fiscal 2021 to fiscal 2022. Additionally, states sometimes make changes to their reporting methods that may affect comparability of data year-over-year. Refer to footnotes in full reports available on NASBO\'s website for details.
# MAGIC
# MAGIC In NASBO’s Fiscal Survey of States, general fund revenue totals reported are based on data states list in their balance sheet information (reported in Tables 3-5 of the full report). For most states, these totals align with the sum of a state’s general fund revenue by tax type (the data reported in this dataset). However, discrepancies sometimes exist due to differing treatment of fund transfers, dedications, revenue-sharing payments, and other adjustments. ';

# COMMAND ----------

excl = ['Total',
       'District of Columbia', 'Guam', 'Puerto Rico',
       'U.S. Virgin Islands', 'Fiscal Survey Edition', 'Median']
pdf = pd.read_excel("/Volumes/mimi_ws_1/nasbo/src/NASBO_State_Rainy_Day_Fund_Balances_Data.xlsx",
                  sheet_name = 'RDF Balances ($ in millions)')
pdf = pd.melt(pdf, id_vars=["State"])
pdf.columns = ["state", "year", "balance_in_dollars"]
pdf['state'] = pdf['state'].str.strip()
pdf= pdf.loc[~pdf['state'].isin(excl),:]

# COMMAND ----------

pdf2 = pd.read_excel("/Volumes/mimi_ws_1/nasbo/src/NASBO_State_Rainy_Day_Fund_Balances_Data.xlsx",
                  sheet_name = 'RDF Balances (% of GF Spending)')
pdf2 = pd.melt(pdf2, id_vars=["State"])
pdf2.columns = ["state", "year", "percent_of_gf"]
pdf2['state'] = pdf2['state'].str.strip()
pdf2= pdf2.loc[~pdf2['state'].isin(excl),:]

# COMMAND ----------

pdf = pd.merge(pdf, pdf2, on=['state', 'year'])

# COMMAND ----------

pdf['balance_in_dollars'] = pd.to_numeric(pdf['balance_in_dollars'])
pdf['percent_of_gf'] = pd.to_numeric(pdf['percent_of_gf'])
pdf['fiscal_year_end_date'] = pdf['year'].apply(lambda x: parse(str(int(x))+'-06-30').date())

# COMMAND ----------

pdf["mimi_src_file_date"] = parse('2023-12-31').date()
pdf['mimi_src_file_name'] = "NASBO_State_Rainy_Day_Fund_Balances_Data.xlsx"
pdf['mimi_dlt_load_date'] = datetime.today().date()
spark.createDataFrame(pdf).write.mode("overwrite").saveAsTable("mimi_ws_1.nasbo.rainy_day_fund_balances")

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE mimi_ws_1.nasbo.rainy_day_fund_balances IS '# State Rainy Day Balances data ($ in Millions).
# MAGIC
# MAGIC ## Data Source
# MAGIC
# MAGIC National Association of State Budget Officers (NASBO), Fall Fiscal Survey of States (1989 - 2023) and Spring Fiscal Survey of States (2024). Survey data were self-reported by executive state budget officers. The source edition for each fiscal year of data is noted at the bottom of the RDF Balances table.
# MAGIC
# MAGIC ## Data Notes
# MAGIC
# MAGIC Rainy Day Fund (RDF) balance amounts are reported in dollars ($) in millions and as a percentage of each state\'s general fund expenditures by fiscal year.
# MAGIC For fiscal 1988-2021, data are provided for the 50 states. Beginning in fiscal 2022, data are also included for the District of Columbia and three U.S. territories (Guam, Puerto Rico, and the Virgin Islands).
# MAGIC Data reported in this compilation represent actuals, except for the two most recent fiscal years in the dataset. Fiscal 2024 represents estimated figures and fiscal 2025 represents projections in governors\' recommended budgets.
# MAGIC Where a state reported a negative rainy day fund balance, this figure is reported as $0 in this dataset.
# MAGIC Some state and aggregate historical figures in this compilation may differ from figures published in previous editions of NASBO\'s Fiscal Survey of States, as some figures have been updated based on a review of original survey data and states\' own documents.
# MAGIC
# MAGIC ## Data Definitions
# MAGIC
# MAGIC In the Fiscal Survey of States, the rainy day fund balance is currently defined as the balance in a state\'s budget stabilization fund(s) or reserve accounts available to supplement general fund spending during a revenue downturn or other unanticipated shortfall (if the specific restrictions on the use of the fund(s) are met). These reserve funds may be stored within or outside of the general fund in the state.';

# COMMAND ----------


