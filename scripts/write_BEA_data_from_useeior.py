# write_BEA_data_from_useeior.py (scripts)
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
The Detail_Use_SUT_2017 was pulled from
USEEIOR's Detail_Use_SUT_2017.rda on 10/12/2023.

The original file is found here:
https://github.com/USEPA/useeior/blob/e02d979f099bbd60625e9c31099f89c111f2d654/data/Detail_Use_SUT_2017.rda

csv obtained by running the following code in Rstudio:
use2017 <- get('Detail_Use_SUT_2017')
write.csv(use2017, file='BEA_Detail_Use_SUT_2017.csv')
CSV manually added to flowsa
The metadata file 'BEA_Detail_Use_SUT_2017_metadata.json' was manually copied from useeior/extdata/metadata

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
The Detail_Supply_2017 was pulled from
USEEIOR's Detail_Supply_2017.rda on 10/12/2023.

The original file is found here:
https://github.com/USEPA/useeior/blob/e02d979f099bbd60625e9c31099f89c111f2d654/data/Detail_Supply_2017.rda

csv obtained by running the following code in Rstudio:
supply2017 <- get('Detail_Supply_2017')
write.csv(supply2017, file='BEA_Detail_Supply_2017.csv')
CSV manually added to flowsa
The metadata file 'BEA_Detail_Supply_2017_metadata.json' was manually copied from useeior/extdata/metadata
---

---
The BEA_Summary_Supply and BEA_Summary_Supply  csvs was pulled from
useeior @a89af7c5adff70ea743e4aab783cfa924a53c0bd using this code

load_all()
for (y in 2017:2022) {

  name <- paste0('Summary_Supply_',y)
  S <- get(name)
  write.csv(S,paste0("BEA_",name,".csv"))

 # name <- paste0('Summary_Use_SUT_',y)
#  S <- get(name)
 # write.csv(S,paste0("BEA_",name,".csv"))

}

The original files are found here:
https://github.com/USEPA/useeior/blob/a89af7c5adff70ea743e4aab783cfa924a53c0bd/data/

The metadata were manually copied from useeior/extdata/metadata
---

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

# pandas2ri.activate()

# useeior = importr('useeior')

# model_with_detail_2012_tables = 'USEEIOv2.0-GHG'
# model = useeior.buildEEIOModel(model_with_detail_2012_tables)

# # Get the UseTransactions object embedded within the BEA data
# UseIndustryTransactions = model.rx2("BEA").rx2("UseTransactions")
# # Convert to a pandas dataframe
# UseIndustryTransactions = pandas2ri.ri2py_dataframe(UseIndustryTransactions)

# # Get the vector of model industries
# Industries = model.rx2("BEA").rx2("Industries")
# # Apply it to the df index
# UseIndustryTransactions.index = Industries
# # Write out to csv
# UseIndustryTransactions.to_csv(f"{externaldatapath}/BEA_2012_Detail_Use_Industry_Transactions.csv")
