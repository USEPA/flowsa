# -*- coding: utf-8 -*-
"""
EPA WARM
"""
import pandas as pd
import flowsa
from flowsa.sectormapping import get_activitytosector_mapping

if __name__ == "__main__":

    ## Read WARM EFs
    warm_factors = (pd.read_csv('https://raw.githubusercontent.com/USEPA/WARMer/main/warmer/data/flowsa_inputs/WARMv15_env.csv')
                    .rename(columns={'ProcessName': 'Activity'})
                    .drop(columns=['ProcessID'])
                    )
    warm_factors['Context'] = warm_factors['Context'].fillna('')

    ### Subset WARM data
    pathway='Landfilling' # pass as function parameter?
    warm_factors = warm_factors.query('Context.str.startswith("emission").values &'\
                                      'ProcessCategory.str.startswith(@pathway).values')

    ### Map WARM to NAICS
    mapping = get_activitytosector_mapping('EPA_WARM')
    warm = warm_factors.merge(mapping, how='left', on='Activity')

    ## Read activity data
    ff = flowsa.getFlowByActivity('EPA_FactsAndFigures', 2018)

    ### MAP F&F to sectors

    ## Merge and multiply activity data based on sectors

