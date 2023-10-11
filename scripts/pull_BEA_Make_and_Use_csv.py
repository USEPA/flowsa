# pull_BEA_Make_and_Use_csv.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
BEA data are imported from useeior and stored as csv files in 'external_data'
folder of flowsa

---
The BEA_Detail_Use_PRO_BeforeRedef was pulled from
USEEIOR's Detail_Use_YEAR_PRO_BeforeRedef.rda on 09/06/2023.

One of the original files is found here:
https://github.com/USEPA/useeior/blob/master/data/Detail_Use_2012_PRO_BeforeRedef.rda

csv obtained by running the following code in Rstudio:
bea <- get('Detail_Use_2012_PRO_BeforeRedef')
write.csv(bea, file='BEA_Detail_Use_2012_PRO_BeforeRedef.csv')

CSV manually added to flowsa

---

The BEA_YEAR_Detail_Make_BeforeRedef was pulled from USEEIOR's
Detail_Make_YEAR_BeforeRedef.rda on 09/06/2023.

One of the original files is found here:
https://github.com/USEPA/useeior/blob/master/data/Detail_Make_2012_BeforeRedef.rda

csv obtained by running the following code in Rstudio:
bea <- get('Detail_Make_2012_BeforeRedef')
write.csv(bea, file='BEA_Detail_Make_2012_BeforeRedef.csv')

CSV manually added to flowsa

---
The BEA_Detail_GrossOutput was pulled from
USEEIOR's Detail_GrossOutput_IO.rda on 09/06/2023.

The original file is found here:
https://github.com/USEPA/useeior/blob/master/data/Detail_GrossOutput_IO.rda

csv obtained by running the following code in Rstudio:
bea <- get('Detail_GrossOutput_IO')
write.csv(bea, file='BEA_Detail_GrossOutput_IO.csv')

CSV manually added to flowsa

"""

# package to read r code no longer working, so reverting to manually
# importing BEA data from useeior

# from rpy2.robjects.packages import importr
# from rpy2.robjects import pandas2ri
# from flowsa.settings import externaldatapath
#
# pandas2ri.activate()
#
# useeior = importr('useeior')
#
# model_with_detail_2012_tables = 'USEEIOv2.0-GHG'
# model = useeior.buildEEIOModel(model_with_detail_2012_tables)
#
# # Get the UseTransactions object embedded within the BEA data
# UseIndustryTransactions = model.rx2("BEA").rx2("UseTransactions")
# # Convert to a pandas dataframe
# UseIndustryTransactions = pandas2ri.ri2py_dataframe(UseIndustryTransactions)
#
# # Get the vector of model industries
# Industries = model.rx2("BEA").rx2("Industries")
# # Apply it to the df index
# UseIndustryTransactions.index = Industries
# # Write out to csv
# UseIndustryTransactions.to_csv(f"{externaldatapath}/BEA_2012_Detail_Use_Industry_Transactions.csv")
