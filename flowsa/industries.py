from typing import Literal
import pandas as pd
from .settings import datapath

naics_crosswalk = pd.read_csv(f'{datapath}NAICS_Crosswalk_TimeSeries.csv',
                              dtype='object')


def naics_key_from_industry_spec(
    industry_spec: dict,
    year: Literal[2002, 2007, 2012, 2017] = 2012
) -> pd.DataFrame:
    '''
    Provides a key for mapping any set of NAICS codes to a given industry
    breakdown, specified in industry_spec. The key is a DataFrame with columns
    'source_naics' and 'target_naics'; it is 1-to-many for any NAICS codes
    shorter than the relevant level given in industry-spec, and many-to-1 for
    any NAICS codes longer than the relevant level.

    The industry_spec is a (possibly nested) dictionary formatted as in this
    example:

    industry_spec = {'default': 'NAICS_3',
                     '112': {'default': 'NAICS_4',
                             '1129': {'default': 'NAICS_6'}},
                     '113': {'default': 'NAICS_4'}}

    This example specification would map any set of NAICS codes to the 3-digit
    level, except that codes in 112 and 113 would be mapped to the 4-digit
    level, with codes in 1129 being mapped to the 6 digits level.

    Some important points in formatting an industry specification:
    1.  Every dictionary in the spec must have a 'default' key, whose value is
        used for any relevant codes not specifically named in the dictionary.
    2.  Each non-default key in a dictionary must be at the length given
        by the default value for the dictionary (so if 'default': 'NAICS_3',
        then any non-default keys must be NAICS codes with exactly 3 digits).
    3.  Each dictionary is applied only to those codes matching its parent
        key (with the root dictionary being applied to all codes).
    '''
    naics_list = (naics_crosswalk
                  [[f'NAICS_{year}_Code']]
                  .rename(columns={f'NAICS_{year}_Code': 'source_naics'}))

    # Find target NAICS by truncating any source_naics longer than the target
    # _naics_level unless industry_spec specifies a longer level for it, in
    # which case send it (recursively) into this function to be truncated to
    # the correct length.
    def _truncate(
        _naics_list: pd.DataFrame,
        _industry_spec: dict
    ) -> pd.DataFrame:
        _naics_level = int(_industry_spec['default'][-1:])
        return pd.concat([
            (_naics_list
             .query('source_naics.str.len() >= @_naics_level'
                    '& source_naics.str.slice(stop=@_naics_level) '
                    'not in @_industry_spec')
             .assign(target_naics=lambda x: x.source_naics.str[:_naics_level])
             ),
            *[
                _truncate(
                    (_naics_list
                     .query('source_naics.str.startswith(@naics)')),
                    _industry_spec[naics]
                )
                for naics in _industry_spec if naics != 'default'
            ]
        ])

    truncated_naics_list = _truncate(naics_list, industry_spec)

    naics_list = naics_list.merge(truncated_naics_list, how='left')

    naics_key = pd.concat([
        naics_list.query('target_naics.notna()'),
        *[
            (naics_list
             .query('target_naics.isna()'
                    '& source_naics.str.len() == @length')
             .drop(columns='target_naics')
             .merge(
                 truncated_naics_list
                 .assign(
                     merge_key=truncated_naics_list.target_naics.str[:length])
                 [['merge_key', 'target_naics']],
                 how='left',
                 left_on='source_naics',
                 right_on='merge_key')
             .drop(columns='merge_key')
             )
            for length in (naics_list.query('target_naics.isna()')
                           .source_naics.str.len().unique())
        ]
    ])

    naics_key = (
        naics_key
        .drop_duplicates()
        .sort_values(by=['source_naics', 'target_naics'])
        .reset_index(drop=True)
    )

    return naics_key
