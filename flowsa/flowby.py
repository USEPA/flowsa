from typing import Callable, List, Optional, Union, Literal, TypeVar
import pandas as pd
import numpy as np
from functools import partial
from . import (common, settings, location, dataclean, metadata, sectormapping,
               literature_values, flowbyactivity, flowbysector, flowsa_yaml,
               validation)
from .flowsa_log import log, vlog
import esupy.processed_data_mgmt
import esupy.dqi

FB = TypeVar('FB', bound='_FlowBy')

with open(settings.datapath + 'flowby_config.yaml') as f:
    flowby_config = flowsa_yaml.load(f)


class _FlowBy(pd.DataFrame):
    def __init__(
        self,
        data: pd.DataFrame or '_FlowBy' = None,
        *args,
        fields: dict = None,
        column_order: List[str] = None,
        string_null: 'np.nan' or None = np.nan,
        **kwargs
    ) -> None:
        if isinstance(data, pd.DataFrame) and fields is not None:
            fill_na_dict = {
                field: 0 if dtype in ['int', 'float'] else pd.NA
                for field, dtype in fields.items()
            }
            na_string_dict = {
                field: {null: string_null
                        for null in ['nan', '<NA>', 'None', '',
                                     np.nan, pd.NA, None]}
                for field, dtype in fields.items() if dtype == 'object'
            }
            data = (data
                    .assign(**{field: None
                            for field in fields if field not in data.columns})
                    .fillna(fill_na_dict)
                    .replace(na_string_dict)
                    .astype(fields))
            data = self._standardize_units(data)
        if isinstance(data, pd.DataFrame) and column_order is not None:
            data = data[[c for c in column_order if c in data.columns]
                        + [c for c in data.columns if c not in column_order]]
        super().__init__(data, *args, **kwargs)

    _metadata = []

    @property
    def _constructor(self) -> '_FlowBy':
        return _FlowBy

    @property
    def _constructor_sliced(self) -> '_FlowBySeries':
        return _FlowBySeries

    @classmethod
    def _getFlowBy(
        cls,
        file_metadata: esupy.processed_data_mgmt.FileMeta,
        download_ok: bool,
        flowby_generator: partial,
        output_path: str
    ) -> '_FlowBy':
        for attempt in ['import local', 'download', 'generate']:
            log.info(
                'Attempting to %s %s %s',
                attempt, file_metadata.name_data, file_metadata.category
            )
            if attempt == 'download' and download_ok:
                esupy.processed_data_mgmt.download_from_remote(
                    file_metadata,
                    settings.paths
                )
            if attempt == 'generate':
                flowby_generator()
            df = esupy.processed_data_mgmt.load_preprocessed_output(
                file_metadata,
                settings.paths
            )
            if df is None:
                log.info(
                    '%s %s not found in %s',
                    file_metadata.name_data,
                    file_metadata.category,
                    output_path
                )
            else:
                log.info(
                    'Successfully loaded %s %s from %s',
                    file_metadata.name_data,
                    file_metadata.category,
                    output_path
                )
                break
        else:
            log.error(
                '%s %s could not be found locally, downloaded, or generated',
                file_metadata.name_data, file_metadata.category
            )
        fb = cls(df)
        return fb

    @staticmethod
    def _standardize_units(fb: pd.DataFrame) -> pd.DataFrame:
        days_in_year = 365
        ft2_to_m2 = 0.092903
        # rounded to match USGS_NWIS_WU mapping file on FEDEFL
        gallon_water_to_kg = 3.79
        ac_ft_water_to_kg = 1233481.84
        acre_to_m2 = 4046.8564224
        mj_in_btu = .0010550559
        m3_to_gal = 264.172
        ton_to_kg = 907.185
        lb_to_kg = 0.45359
        exchange_rate = float(
            literature_values
            .get_Canadian_to_USD_exchange_rate(str(fb.Year.unique()[0]))
        )

        fb = fb.assign(Unit=fb.Unit.str.strip())

        standardized = (
            fb
            .assign(
                FlowAmount=(
                    fb.FlowAmount
                    .mask(fb.Unit.isin(['ACRES', 'Acres']),
                          fb.FlowAmount * acre_to_m2)
                    .mask(fb.Unit.isin(['million sq ft',
                                        'million square feet']),
                          fb.FlowAmount * 10**6 * ft2_to_m2)
                    .mask(fb.Unit.isin(['square feet']),
                          fb.FlowAmount * ft2_to_m2)
                    .mask(fb.Unit.isin(['Canadian Dollar']),
                          fb.FlowAmount / exchange_rate)
                    .mask(fb.Unit.isin(['gallons/animal/day']),
                          fb.FlowAmount * gallon_water_to_kg * days_in_year)
                    .mask(fb.Unit.isin(['ACRE FEET / ACRE']),
                          fb.FlowAmount / acre_to_m2 * ac_ft_water_to_kg)
                    .mask(fb.Unit.isin(['Mgal']),
                          fb.FlowAmount * 10**6 * gallon_water_to_kg)
                    .mask(fb.Unit.isin(['gal', 'gal/USD']),
                          fb.FlowAmount * gallon_water_to_kg)
                    .mask(fb.Unit.isin(['Bgal/d']),
                          fb.FlowAmount
                          * 10**9 * gallon_water_to_kg * days_in_year)
                    .mask(fb.Unit.isin(['Mgal/d']),
                          fb.FlowAmount
                          * 10**6 * gallon_water_to_kg * days_in_year)
                    .mask(fb.Unit.isin(['Quadrillion Btu']),
                          fb.FlowAmount * 10**15 * mj_in_btu)
                    .mask(fb.Unit.isin(['Trillion Btu', 'TBtu']),
                          fb.FlowAmount * 10**12 * mj_in_btu)
                    .mask(fb.Unit.isin(['million Cubic metres/year']),
                          fb.FlowAmount
                          * 10**6 * m3_to_gal * gallon_water_to_kg)
                    .mask(fb.Unit.isin(['TON']),
                          fb.FlowAmount * ton_to_kg)
                    .mask(fb.Unit.isin(['LB']),
                          fb.FlowAmount * lb_to_kg)
                ),
                Unit=(
                    fb.Unit
                    .mask(fb.Unit.isin(['ACRES', 'Acres', 'million sq ft',
                                        'million square feet',
                                        'square feet']),
                          'm2')
                    .mask(fb.Unit.isin(['Canadian Dollar']),
                          'USD')
                    .mask(fb.Unit.isin(['gallons/animal/day', 'Mgal', 'gal',
                                        'Bgal/d', 'Mgal/d',
                                        'million Cubic metres/year',
                                        'TON', 'LB']),
                          'kg')
                    .mask(fb.Unit.isin(['ACRE FEET / ACRE']),
                          'kg/m2')
                    .mask(fb.Unit.isin(['gal/USD']),
                          'kg/USD')
                    .mask(fb.Unit.isin(['Quadrillion Btu',
                                        'Trillion Btu', 'TBtu']),
                          'MJ')
                )
            )
        )

        return standardized

    def conditional_pipe(
        self: FB,
        condition: bool,
        function: Callable,
        *args, **kwargs
    ) -> FB:
        '''
        Similar to pandas .pipe() method, but first checks if the given
        condition is true. If it is not, then the object that called
        conditional_pipe is returned unchanged. Additional args and kwargs
        are passed to function
        :param condition: bool, condition under which the given function should
            be called
        :param function: Callable, function that expects a DataFrame or _FlowBy
            as the first argument
        :return: function(self, *args, **kwargs) if condition is True,
            else self
        '''
        if condition:
            return function(self, *args, **kwargs)
        else:
            return self

    def conditional_method(
        self: FB,
        condition: bool,
        method: str,
        *args, **kwargs
    ) -> FB:
        '''
        Conditionally calls the specified method of the calling FlowBy.
        Additional args and kwargs are passed to the method
        :param condition: bool, condition under which the given method should
            be called
        :param function: str, name of FlowBy or DataFrame method
        :return: self.method(*args, **kwargs) if condition is True, else self
        '''
        if condition:
            return getattr(self, method)(*args, **kwargs)
        else:
            return self

    def standardize_units(self: FB) -> FB:
        """
        Standardizes units. Timeframe is annual.
        :return: FlowBy dataframe, with standarized units
        """
        return self._standardize_units(self)

    def update_fips_to_geoscale(
        self: FB,
        to_geoscale: str,
    ) -> FB:
        """
        Sets FIPS codes to 5 digits by zero-padding FIPS codes at the specified
        geoscale on the right (county geocodes are unmodified, state codes are
        generally padded with 3 zeros, and the "national" FIPS code is set to
        00000, the value in flowsa.location.US_FIPS)
        :param to_geoscale: str, target geoscale
        :return: FlowBy dataset with 5 digit fips
        """
        if to_geoscale == 'national':
            return self.assign(Location=location.US_FIPS)
        elif to_geoscale == 'state':
            return (self
                    .assign(Location=self.Location.apply(
                        lambda x: str(x)[:2].ljust(5, '0'))))
        else:
            return self


class FlowByActivity(_FlowBy):
    def __init__(
        self,
        data: pd.DataFrame or '_FlowBy' = None,
        *args,
        mapped: bool = False,
        w_sector: bool = False,
        **kwargs
    ) -> None:
        if isinstance(data, pd.DataFrame):
            mapped = mapped or any(
                [c in data.columns for c in flowby_config['_mapped_fields']]
            )
            w_sector = w_sector or any(
                [c in data.columns for c in flowby_config['_sector_fields']]
            )

            if mapped and w_sector:
                fields = flowby_config['fba_mapped_w_sector_fields']
            elif mapped:
                fields = flowby_config['fba_mapped_fields']
            elif w_sector:
                fields = flowby_config['fba_w_sector_fields']
            else:
                fields = flowby_config['fba_fields']

            column_order = flowby_config['fba_column_order']
        else:
            fields = None
            column_order = None

        super().__init__(data,
                         fields=fields,
                         column_order=column_order,
                         *args, **kwargs)

    _metadata = [*_FlowBy()._metadata]

    @property
    def _constructor(self) -> 'FlowByActivity':
        return FlowByActivity

    @property
    def _constructor_sliced(self) -> '_FBASeries':
        return _FBASeries

    @classmethod
    def getFlowByActivity(
        cls,
        source: str,
        year: int = None,
        download_ok: bool = settings.DEFAULT_DOWNLOAD_IF_MISSING
    ) -> 'FlowByActivity':
        """
        Loads stored data in the FlowByActivity format. If it is not
        available, tries to download it from EPA's remote server (if
        download_ok is True), or generate it.
        :param datasource: str, the code of the datasource.
        :param year: int, a year, e.g. 2012
        :param download_ok: bool, if True will attempt to load from
            EPA remote server prior to generating
        :return: a FlowByActivity dataframe
        """
        file_metadata = metadata.set_fb_meta(
            source if year is None else f'{source}_{year}',
            'FlowByActivity'
        )
        flowby_generator = partial(
            flowbyactivity.main,
            source=source,
            year=year
        )
        fb = super()._getFlowBy(
            file_metadata=file_metadata,
            download_ok=download_ok,
            flowby_generator=flowby_generator,
            output_path=settings.fbaoutputpath
        )
        fba = cls(fb)
        return fba


class FlowBySector(_FlowBy):
    def __init__(
        self,
        data: pd.DataFrame or '_FlowBy' = None,
        *args,
        collapsed: bool = False,
        w_activity: bool = False,
        **kwargs
    ) -> None:
        if isinstance(data, pd.DataFrame):
            collapsed = collapsed or any(
                [c in data.columns for c in flowby_config['_collapsed_fields']]
            )
            w_activity = w_activity or any(
                [c in data.columns for c in flowby_config['_activity_fields']]
            )

            if collapsed:
                fields = flowby_config['fbs_collapsed_fields']
            elif w_activity:
                fields = flowby_config['fbs_w_activity_fields']
            else:
                fields = flowby_config['fbs_fields']

            column_order = flowby_config['fbs_column_order']
        else:
            fields = None
            column_order = None

        super().__init__(data,
                         fields=fields,
                         column_order=column_order,
                         *args, **kwargs)

    _metadata = [*_FlowBy()._metadata]

    @property
    def _constructor(self) -> 'FlowBySector':
        return FlowBySector

    @property
    def _constructor_sliced(self) -> '_FBSSeries':
        return _FBSSeries

    @classmethod
    def getFlowBySector(
        cls,
        method: str,
        external_config_path: str = None,
        download_sources_ok: bool = settings.DEFAULT_DOWNLOAD_IF_MISSING,
        download_fbs_ok: bool = settings.DEFAULT_DOWNLOAD_IF_MISSING
    ) -> 'FlowBySector':
        """
        Loads stored FlowBySector output. If it is not
        available, tries to download it from EPA's remote server (if
        download_ok is True), or generate it.
        :param method: string, name of the FBS attribution method file to use
        :param external_config_path: str, path to the FBS method file if
            loading a file from outside the flowsa repository
        :param download_fba_ok: bool, if True will attempt to load FBAs
            used in generating the FBS from EPA's remote server rather than
            generating (if not found locally)
        :param download_FBS_if_missing: bool, if True will attempt to load the
            the FBS from EPA's remote server rather than generating it
            (if not found locally)
        :return: FlowBySector dataframe
        """
        file_metadata = metadata.set_fb_meta(method, 'FlowBySector')
        flowby_generator = partial(
            flowbysector.main,
            method=method,
            fbsconfigpath=external_config_path,
            download_FBAs_if_missing=download_sources_ok
        )
        fb = super()._getFlowBy(
            file_metadata=file_metadata,
            download_ok=download_fbs_ok,
            flowby_generator=flowby_generator,
            output_path=settings.fbsoutputpath
        )
        fbs = cls(fb)
        return fbs

    @classmethod
    def generateFlowBySector(
        cls,
        method: str,
        external_config_path: str = None,
        download_sources_ok: bool = settings.DEFAULT_DOWNLOAD_IF_MISSING,
    ) -> 'FlowBySector':
        '''
        Generates a FlowBySector dataset.
        :param method: str, name of FlowBySector method .yaml file to use.
        :param external_config_path: str, optional. If given, tells flowsa
            where to look for the method yaml specified above.
        :param download_fba_ok: bool, optional. Whether to attempt to download
            source data FlowByActivity files from EPA server rather than
            generating them.
        '''
        log.info('Beginning FlowBySector generation for %s', method)
        method_config = common.load_yaml_dict(method, 'FBS',
                                              external_config_path)
        sources = method_config['source_names']

        component_fbs_list = []
        for source_name, source_config in sources.items():
            if source_config['data_format'] in ['FBS', 'FBS_outside_flowsa']:
                if source_config['data_format'] == 'FBS_outside_flowsa':
                    source_data = FlowBySector(
                        source_config['FBS_datapull_fxn'](source_config,
                                                          method_config,
                                                          external_config_path)
                    )

                else:  # TODO: Test this section.
                    source_data = FlowBySector.getFlowBySector(
                        source=source_name,
                        external_config_path=external_config_path,
                        download_sources_ok=download_sources_ok,
                        download_fbs_ok=download_sources_ok
                    )

                fbs = (source_data
                       .conditional_pipe(
                           'clean_fbs_df_fxn' in source_config,
                           source_config.get('clean_fbs_df_fxn'))
                       .update_fips_to_geoscale(
                           method_config['target_geoscale']))
                log.info('Appending %s to FBS list', source_name)
                component_fbs_list.append(fbs)

        return component_fbs_list


# The three classes extending pd.Series, together with the _constructor...
# methods of each class, are required for allowing pandas methods called on
# objects of these classes to return objects of these classes, as desired.
# For more information, see
# https://pandas.pydata.org/docs/development/extending.html

class _FlowBySeries(pd.Series):
    _metadata = [*_FlowBy()._metadata]

    @property
    def _constructor(self) -> '_FlowBySeries':
        return _FlowBySeries

    @property
    def _constructor_expanddim(self) -> '_FlowBy':
        return _FlowBy


class _FBASeries(pd.Series):
    _metadata = [*FlowByActivity()._metadata]

    @property
    def _constructor(self) -> '_FBASeries':
        return _FBASeries

    @property
    def _constructor_expanddim(self) -> 'FlowByActivity':
        return FlowByActivity


class _FBSSeries(pd.Series):
    _metadata = [*FlowBySector()._metadata]

    @property
    def _constructor(self) -> '_FBSSeries':
        return _FBSSeries

    @property
    def _constructor_expanddim(self) -> 'FlowBySector':
        return FlowBySector
