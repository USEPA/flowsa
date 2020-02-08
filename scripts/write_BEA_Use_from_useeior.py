"""
A script to get Use table transactions from a useeior EEIOmodel and store them as .csv
Depends on rpy2 and tzlocal as well as having R installed and useeior installed
"""

from flowsa.common import datapath
from rpy2.robjects.packages import importr
from rpy2.robjects import pandas2ri
pandas2ri.activate()

useeior = importr('useeior')

model_with_detail_2012_tables = 'USEEIOv2.0-GHG'
model = useeior.buildEEIOModel(model_with_detail_2012_tables)

#Get the UseTransactions object embedded within the BEA data
UseIndustryTransactions = model.rx2("BEA").rx2("UseTransactions")
#Convert to a pandas dataframe
UseIndustryTransactions = pandas2ri.ri2py_dataframe(UseIndustryTransactions)

#Get the vector of model industries
Industries = model.rx2("BEA").rx2("Industries")
#Apply it to the df index
UseIndustryTransactions.index = Industries
#Write out to csv
UseIndustryTransactions.to_csv(datapath + "BEA_2012_Detail_Use_Industry_Transactions.csv")


