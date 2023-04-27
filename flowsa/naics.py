from typing import Literal
import numpy as np
import pandas as pd
from . import settings


def industry_spec_key(
    industry_spec: dict,
    year: Literal[2002, 2007, 2012, 2017] = 2012
) -> pd.DataFrame:
    """
    Provides a key for mapping any set of NAICS codes to a given industry
    breakdown, specified in industry_spec. The key is a DataFrame with columns
    'source_naics' and 'target_naics'; it is 1-to-many for any NAICS codes
    shorter than the relevant level given in industry-spec, and many-to-1 for
    any NAICS codes longer than the relevant level.

    The industry_spec is a (possibly nested) dictionary formatted as in this
    example:

    industry_spec = {'default': 'NAICS_3',
                     'NAICS_4': ['112', '113'],
                     'NAICS_6': ['1129']

    This example specification would map any set of NAICS codes to the 3-digit
    level, except that codes in 112 and 113 would be mapped to the 4-digit
    level, with codes in 1129 being mapped to the 6 digits level.

    The top industry_spec dictionary may also include a key 'non_naics', where
    the associated value is a non-NAICS "industry" or list of such "industries"
    that should be included in the set of industries that can be mapped to.
    In this case, the user will need to supply their own crosswalk which maps
    activities to that industry.

    Some important points in formatting an industry specification:
    1.  Every dictionary in the spec must have a 'default' key, whose value is
        used for any relevant codes not specifically named in the dictionary.
    2.  Each non-default key in a dictionary must be at the length given
        by the default value for the dictionary (so if 'default': 'NAICS_3',
        then any non-default keys must be NAICS codes with exactly 3 digits).
    3.  Each dictionary is applied only to those codes matching its parent
        key (with the root dictionary being applied to all codes).
    """
    naics_crosswalk = pd.read_csv(
        f'{settings.datapath}NAICS_2012_Crosswalk.csv', dtype='object'
    )

    naics = naics_crosswalk.assign(
        target_naics=naics_crosswalk[industry_spec['default']])
    for k, v in industry_spec.items():
        if k not in ['default', 'non_naics']:
            naics = naics.assign(
                target_naics=naics.target_naics.mask(
                    naics.drop(columns='target_naics').isin(v).any(axis='columns'),
                    naics[k]
                )
            )
    # melt the dataframe to include source naics
    naics_key = naics.melt(id_vars="target_naics", value_name="source_naics")
    # add user-specified non-naics
    if 'non_naics' in industry_spec:
        non_naics = industry_spec['non_naics']
        if isinstance(non_naics, str):
            non_naics = [non_naics]
        naics_key = pd.concat([naics_key,
                               pd.DataFrame({'source_naics': non_naics,
                                             'target_naics': non_naics})])

    # drop source_naics that are more aggregated than target_naics, reorder
    naics_key = (naics_key[['source_naics', 'target_naics']]
                 .dropna()
                 .drop_duplicates()
                 .sort_values(by=['source_naics', 'target_naics'])
                 .reset_index(drop=True)
                 )

    return naics_key


def year_crosswalk(
    source_year: Literal[2002, 2007, 2012, 2017],
    target_year: Literal[2002, 2007, 2012, 2017]
) -> pd.DataFrame:
    '''
    Provides a key for switching between years of the NAICS specification.

    :param source_year: int, one of 2002, 2007, 2012, or 2017.
    :param target_year: int, one of 2002, 2007, 2012, or 2017.
    :return: pd.DataFrame with columns 'source_naics' and 'target_naics',
        corresponding to NAICS codes for the source and target specifications.
    '''
    return (
        pd.read_csv(f'{settings.datapath}NAICS_Crosswalk_TimeSeries.csv',
                    dtype='object')
        .assign(source_naics=lambda x: x[f'NAICS_{source_year}_Code'],
                target_naics=lambda x: x[f'NAICS_{target_year}_Code'])
        [['source_naics', 'target_naics']]
        .drop_duplicates()
        .reset_index(drop=True)
    )
