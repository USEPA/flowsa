from typing import Literal
import enum
from functools import total_ordering
import pandas as pd
from . import settings
from .flowsa_log import log


@total_ordering
class scale(enum.Enum):
    '''
    Enables the representation of geoscales as constants which can
    be compared using <, >, max(), min(), etc. Note that "larger" implies
    more aggregated.
    '''
    NATIONAL = 5, True
    CENSUS_REGION = 4, False
    CENSUS_DIVISION = 3, False
    STATE = 2, True
    COUNTY = 1, True

    def __init__(self, aggregation_level: int, has_fips_level: bool) -> None:
        self.aggregation_level = aggregation_level
        self.has_fips_level = has_fips_level

    def __lt__(self, other):
        if other.__class__ is self.__class__:
            return self.aggregation_level < other.aggregation_level
        elif other in [float('inf'), float('-inf')]:
            # ^^^ Add np.nan to list if such comparison is needed.
            return self.aggregation_level < other
            # ^^^ Enables pandas max and min functions to work even if
            # there are missing values (with dropna=True)
        else:
            return NotImplemented

    @classmethod
    def from_string(
        cls,
        geoscale: Literal['national', 'census_region', 'census_division',
                          'state', 'county']
    ) -> 'scale':
        '''
        Return the appropriate geo.scale constant given a (non-case-sensitive)
        string
        :param geoscale: str
        :return: geo.scale constant
        '''
        geoscale = geoscale.lower()
        if geoscale == 'national':
            return cls.NATIONAL
        elif geoscale == 'census_region':
            return cls.CENSUS_REGION
        elif geoscale == 'census_division':
            return cls.CENSUS_DIVISION
        elif geoscale == 'state':
            return cls.STATE
        elif geoscale == 'county':
            return cls.COUNTY
        else:
            raise ValueError(f'No geo.scale level corresponds to {geoscale}')


def get_all_fips(year: Literal[2010, 2013, 2015] = 2015) -> pd.DataFrame:
    '''
    Read fips based on year specified, year defaults to 2015
    :param year: int, one of 2010, 2013, or 2015, default year is 2015
        because most recent year of FIPS available
    :return: df, with columns=['State', 'FIPS', 'County'] for specified year.
        'State' is NaN for national level FIPS ('00000'), and 'County'
        is Nan for national and each state level FIPS.
    '''
    return (pd
            .read_csv(settings.datapath / 'FIPS_Crosswalk.csv',
                      header=0, dtype=object)
            [['State', f'FIPS_{year}', f'County_{year}', 'FIPS_Scale']]
            .rename(columns={f'FIPS_{year}': 'FIPS',
                             f'County_{year}': 'County'})
            .sort_values('FIPS')
            .reset_index(drop=True))


def filtered_fips(
        geoscale: Literal['national', 'state', 'county',
                          scale.NATIONAL, scale.STATE, scale.COUNTY],
        year: Literal[2010, 2013, 2015] = 2015
    ) -> pd.DataFrame:
    if geoscale == 'national' or geoscale == scale.NATIONAL:
        return (get_all_fips(year).query('State.isnull()').drop(columns='FIPS_Scale'))
    elif geoscale == 'state' or geoscale == scale.STATE:
        return (get_all_fips(year).query('State.notnull() & County.isnull()').drop(columns='FIPS_Scale'))
    elif geoscale == 'county' or geoscale == scale.COUNTY:
        return (get_all_fips(year).query('County.notnull()').drop(columns='FIPS_Scale'))
    else:
        log.error('No FIPS list exists for the given geoscale: %s', geoscale)
        raise ValueError(geoscale)
