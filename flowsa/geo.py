from typing import Literal
import enum
from functools import total_ordering
import pandas as pd
import numpy as np
from . import settings
from .flowsa_log import log


# TODO: Evaluate whether an UNKNOWN entry should be included, with np.nan
# TODO: as the aggregation_level. Doing so will also require implementing
# TODO: __gt__(), __le__(), and __ge__() to return the proper comparisons.
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
        else:
            return NotImplemented

    @classmethod
    def from_string(
        cls,
        geoscale: Literal['national', 'census_region', 'census_division',
                          'state', 'county']
    ) -> 'scale':
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
    """
    Read fips based on year specified, year defaults to 2015
    :param year: int, one of 2010, 2013, or 2015, default year is 2015
        because most recent year of FIPS available
    :return: df, FIPS for specified year
    """
    return (pd
            .read_csv(settings.datapath + 'FIPS_Crosswalk.csv',
                      header=0, dtype=object)
            [['State', f'FIPS_{year}', f'County_{year}']]
            .rename(columns={f'FIPS_{year}': 'FIPS',
                             f'County_{year}': 'County'})
            .sort_values('FIPS')
            .reset_index(drop=True))


def filtered_fips_list(
    geoscale: Literal['national', 'state', 'county',
                      scale.NATIONAL, scale.STATE, scale.COUNTY],
    year: Literal[2010, 2013, 2015] = 2015
) -> pd.DataFrame:
    if geoscale == 'national' or geoscale == scale.NATIONAL:
        return (list(get_all_fips(year).query('State.isnull()').FIPS))
    elif geoscale == 'state' or geoscale == scale.STATE:
        return (list(get_all_fips(year)
                     .query('State.notnull() & County.isnull()').FIPS))
    elif geoscale == 'county' or geoscale == scale.COUNTY:
        return (list(get_all_fips(year).query('County.notnull()').FIPS))
    else:
        log.error('No FIPS list exists for the given geoscale: %s', geoscale)
        raise ValueError(geoscale)
