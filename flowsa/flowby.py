from typing import Callable, List, Literal, TypeVar
import pandas as pd
import numpy as np
from functools import partial, reduce
from . import (common, settings, location, dataclean, metadata, sectormapping,
               literature_values, flowbyactivity, flowbysector, flowsa_yaml,
               validation, geo, industries, fbs_allocation)
from .flowsa_log import log, vlog
import esupy.processed_data_mgmt
import esupy.dqi
import fedelemflowlist

FB = TypeVar('FB', bound='_FlowBy')
S = TypeVar('S', bound='_FlowBySeries')

with open(settings.datapath + 'flowby_config.yaml') as f:
    flowby_config = flowsa_yaml.load(f)


class _FlowBy(pd.DataFrame):
    _metadata = ['source_name', 'source_config',
                 'activity_set', 'activity_config']

    source_name: str
    source_config: dict
    activity_set: str
    activity_config: dict

    def __init__(
        self,
        data: pd.DataFrame or '_FlowBy' = None,
        *args,
        fields: dict = None,
        column_order: List[str] = None,
        string_null: 'np.nan' or None = np.nan,
        **kwargs
    ) -> None:
        # Assign values to metadata attributes, checking the following sources,
        # in order: self, data, kwargs; then defaulting to an empty version of
        # the type specified in the type hints, or None.
        for attribute in self._metadata:
            if not hasattr(self, attribute):
                super().__setattr__(
                    attribute,
                    getattr(data, attribute,
                            kwargs.pop(attribute,
                                       self.__annotations__.get(attribute,
                                                                None)()))
                )

        if isinstance(data, pd.DataFrame) and fields is not None:
            fill_na_dict = {
                field: 0 if dtype in ['int', 'float'] else string_null
                for field, dtype in fields.items()
            }
            null_string_dict = {
                field: {null: string_null
                        for null in ['nan', '<NA>', 'None', '',
                                     np.nan, pd.NA, None]}
                for field, dtype in fields.items() if dtype == 'object'
            }
            data = (data
                    .assign(**{field: None
                            for field in fields if field not in data.columns})
                    .fillna(fill_na_dict)
                    .replace(null_string_dict)
                    .astype(fields))
            data = self._standardize_units(data)
        if isinstance(data, pd.DataFrame) and column_order is not None:
            data = data[[c for c in column_order if c in data.columns]
                        + [c for c in data.columns if c not in column_order]]
        super().__init__(data, *args, **kwargs)

    @property
    def _constructor(self) -> '_FlowBy':
        return _FlowBy

    @property
    def _constructor_sliced(self) -> '_FlowBySeries':
        return _FlowBySeries

    def __finalize__(self, other, method=None, **kwargs):
        '''
        Determines how metadata is propagated when using DataFrame methods.
        super().__finalize__() takes care of methods involving only one FlowBy
        object. Additional code below specifies how to propagate metadata under
        other circumstances, such as merging.

        merge: use metadata of left dataframe
        '''
        self = super().__finalize__(other, method=method, **kwargs)

        # merge operation: using metadata of the left object
        if method == "merge":
            for name in self._metadata:
                object.__setattr__(self, name, getattr(other.left, name, None))
        return self

    @property
    def flow_col(self) -> str:
        return 'Flowable' if 'Flowable' in self else 'FlowName'

    @property
    def groupby_cols(self) -> List[str]:
        return [x for x in self if self[x].dtype in ['int', 'object']]

    @classmethod
    def _getFlowBy(
        cls,
        file_metadata: esupy.processed_data_mgmt.FileMeta,
        download_ok: bool,
        flowby_generator: partial,
        output_path: str,
        *,
        source_name: str = None,
        source_config: dict = None,
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
        fb = cls(df, source_name=source_name, source_config=source_config)
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

        standardized_units = ['m2', 'USD', 'kg', 'kg/m2', 'kg/USD', 'MJ']

        if any(~standardized.Unit.isin(standardized_units)):
            log.warning('Some units not standardized on import: %s. May not '
                        'be a problem, if they will be standardized later, '
                        'e.g. by mapping to the federal elementary flow list',
                        [unit for unit in standardized.Unit.unique()
                         if unit not in standardized_units])

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
        to_geoscale: Literal['national', 'state', 'county',
                             geo.scale.NATIONAL, geo.scale.STATE,
                             geo.scale.COUNTY],
    ) -> FB:
        """
        Sets FIPS codes to 5 digits by zero-padding FIPS codes at the specified
        geoscale on the right (county geocodes are unmodified, state codes are
        generally padded with 3 zeros, and the "national" FIPS code is set to
        00000, the value in flowsa.location.US_FIPS)
        :param to_geoscale: str, target geoscale
        :return: FlowBy dataset with 5 digit fips
        """
        if type(to_geoscale) == str:
            to_geoscale = geo.scale.from_string(to_geoscale)

        if to_geoscale == geo.scale.NATIONAL:
            return (self
                    .assign(Location=(geo.filtered_fips('national')
                                      .FIPS.values[0])))
        elif to_geoscale == geo.scale.STATE:
            return (self
                    .assign(Location=self.Location.apply(
                        lambda x: str(x)[:2].ljust(5, '0'))))
        elif to_geoscale == geo.scale.COUNTY:
            return self
        else:
            log.error('No FIPS level corresponds to the given geoscale: %s',
                      to_geoscale)

    def aggregate_flowby(
        self: FB,
        columns_to_group_by: List[str] = None,
        columns_to_average: List[str] = None
    ) -> FB:
        """
        Aggregates (sums) FlowBy 'FlowAmount' column based on group_by_columns
        and generates weighted average values based on FlowAmount values
        for other columns
        :param columns_to_group_by: list, names of columns to group by. If not
            provided, all columns not of 'float' data type will be used, except
            'Description'.
        :param columns_to_average: list, names of columns for which an average,
            weighted by 'FlowAmount', should be calculated. If not provided,
            all columns of 'float' data type will be used, except 'FlowAmount'.
        :return: FlowBy, with aggregated columns
        """
        if columns_to_group_by is None:
            columns_to_group_by = [
                x for x in self.columns
                if self[x].dtype != 'float' and x != 'Description'
            ]
        if columns_to_average is None:
            columns_to_average = [
                x for x in self.columns
                if self[x].dtype == 'float' and x != 'FlowAmount'
            ]

        fb = (
            self
            .query('FlowAmount != 0')
            .drop(columns=[c for c in self.columns
                           if c not in ['FlowAmount',
                                        *columns_to_average,
                                        *columns_to_group_by]])
        )

        aggregated = (
            fb
            .assign(
                **{f'_{c}_weighted': fb[c] * fb.FlowAmount
                   for c in columns_to_average},
                **{f'_{c}_weights': fb.FlowAmount * fb[c].notnull()
                   for c in columns_to_average}
            )
            .groupby(columns_to_group_by, dropna=False)
            .agg(sum)
            .reset_index()
        )
        aggregated = (
            aggregated
            .assign(
                **{c: (aggregated[f'_{c}_weighted']
                       / aggregated[f'_{c}_weights'])
                   for c in columns_to_average}
            )
            .drop(
                columns=([*[f'_{c}_weighted' for c in columns_to_average],
                          *[f'_{c}_weights' for c in columns_to_average]])
            )
        )
        return aggregated

    def add_primary_secondary_sectors(self: FB) -> FB:
        '''
        This function adds to the calling dataframe 'PrimarySector' and
        'SecondarySector' columns based on the 'SectorProducedBy' and
        'SectorConsumedBy' columns, and logic based on the type of flow. The
        original dataset is returned unchanged if it lacks 'SectorProducedBy'
        or 'SectorConsumedBy' columns.

        If the flow type is TECHNOSPHERE_FLOW, the primary sector is
        SectorConsumedBy. Otherwise, it is SectorProducedBy unless only
        SectorConsumedBy is given, or if both are given and SectorProducedBy
        is one of 22, 221, 2213, 22131, or 221310 AND SectorConsumedBy is one
        of F010, F0100, or F01000.

        In all cases, the secondary sector is the "other" sector if both are
        given. In many cases, only one of SectorProducedBy or SectorConsumedBy
        is given, and therefore SecondarySector is null.

        :return: FlowBy dataset, with 'PrimarySector' and 'SecondarySector'
            columns added, if possible; otherwise, the unmodified caling FlowBy
            dataset.
        '''
        if 'SectorProducedBy' not in self or 'SectorConsumedBy' not in self:
            log.error('Cannot add PrimarySector or SecondarySector columns, '
                      'since SectorProducedBy and/or SectorConsumedBy columns '
                      'are missing.')
            return self
        else:
            log.info('Adding PrimarySector and SecondarySector columns from '
                     'SectorProducedBy and SectorConsumedBy columns.')
            fb = self.assign(
                PrimarySector=self.SectorProducedBy.mask(
                    (self.FlowType == 'TECHNOSPHERE_FLOW')
                    | (self.SectorProducedBy.isna())
                    | (self.SectorProducedBy.isin(
                            ['22', '221', '2213', '22131', '221310']
                        )
                        & self.SectorConsumedBy.isin(
                            ['F010', 'F0100', 'F01000']
                        )),
                    self.SectorConsumedBy
                )
            )

            def _identify_secondary_sector(row: _FlowBySeries) -> str:
                sectors = [row.SectorProducedBy, row.SectorConsumedBy]
                sectors.remove(row.PrimarySector)
                return sectors[0]

            fb = fb.assign(
                SecondarySector=(
                    fb.apply(_identify_secondary_sector, axis='columns')
                    # ^^^ Applying with axis='columns' applies TO each row.
                    .astype('object'))
            )

            return fb


class FlowByActivity(_FlowBy):
    _metadata = [*_FlowBy()._metadata]

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

    @property
    def _constructor(self) -> 'FlowByActivity':
        return FlowByActivity

    @property
    def _constructor_sliced(self) -> '_FBASeries':
        return _FBASeries

    @classmethod
    def getFlowByActivity(
        cls,
        source_name: str,
        year: int = None,
        download_ok: bool = settings.DEFAULT_DOWNLOAD_IF_MISSING,
        **kwargs
    ) -> 'FlowByActivity':
        """
        Loads stored data in the FlowByActivity format. If it is not
        available, tries to download it from EPA's remote server (if
        download_ok is True), or generate it.
        :param datasource: str, the code of the datasource.
        :param year: int, a year, e.g. 2012
        :param download_ok: bool, if True will attempt to load from
            EPA remote server prior to generating
        :kwargs: keyword arguments to pass to _getFlowBy(). Possible kwargs
            include source_config.
        :return: a FlowByActivity dataframe
        """
        file_metadata = metadata.set_fb_meta(
            source_name if year is None else f'{source_name}_{year}',
            'FlowByActivity'
        )
        flowby_generator = partial(
            flowbyactivity.main,
            source=source_name,
            year=year
        )
        return super()._getFlowBy(
            file_metadata=file_metadata,
            download_ok=download_ok,
            flowby_generator=flowby_generator,
            output_path=settings.fbaoutputpath,
            source_name=source_name,
            **kwargs
        )

    # TODO: probably only slight modification is needed to allow for material
    # flow list mapping using this function as well.
    def map_to_fedefl_list(
        self: 'FlowByActivity',
        drop_fba_columns: bool = False,
        drop_unmapped_rows: bool = False
    ) -> 'FlowByActivity':
        log.info('Mapping flows in %s to federal elementary flow list',
                 self.source_name)

        fba_merge_keys = [
            'SourceName',
            'Flowable',
            'Unit',
            'Context'
        ]
        mapping_subset = self.source_config.get('fedefl_mapping',
                                                self.source_name)
        mapping_fields = [
            'SourceListName',
            'SourceFlowName',
            'SourceFlowContext',
            'SourceUnit',
            'ConversionFactor',
            'TargetFlowName',
            'TargetFlowContext',
            'TargetUnit',
            'TargetFlowUUID'
        ]
        mapping_merge_keys = [
            'SourceListName',
            'SourceFlowName',
            'SourceUnit',
            'SourceFlowContext'
        ]
        merge_type = 'inner' if drop_unmapped_rows else 'left'
        if 'fedefl_mapping' in self.source_config:
            fba_merge_keys.remove('SourceName')
            mapping_merge_keys.remove('SourceListName')

        if any(self.Unit.str.contains('/d')):
            log.info('Converting daily flows %s to annual',
                     [unit for unit in self.Unit.unique() if '/d' in unit])
        fba = (
            self
            .assign(Flowable=self.FlowName,
                    Context=self.Compartment,
                    FlowAmount=self.FlowAmount.mask(
                        self.Unit.str.contains('/d'),
                        self.FlowAmount * 365),
                    Unit=self.Unit.str.replace('/d', ''))
            .conditional_method(drop_fba_columns, 'drop',
                                columns=['FlowName', 'Compartment'])
        )

        mapping = (
            fedelemflowlist.get_flowmapping(mapping_subset)[mapping_fields]
            .assign(ConversionFactor=lambda x: x.ConversionFactor.fillna(1))
        )
        if mapping.empty:
            log.warning('Elementary flow list entries for %s not found',
                        mapping_subset)
            return FlowByActivity(self, mapped=True)

        mapped_fba = fba.merge(mapping,
                               how=merge_type,
                               left_on=fba_merge_keys,
                               right_on=mapping_merge_keys,
                               indicator='mapped')

        mapped_fba = (mapped_fba
                      .assign(
                          Flowable=mapped_fba.Flowable.mask(
                              mapped_fba.TargetFlowName.notnull(),
                              mapped_fba.TargetFlowName),
                          Context=mapped_fba.Context.mask(
                              mapped_fba.TargetFlowName.notnull(),
                              mapped_fba.TargetFlowContext),
                          Unit=mapped_fba.Unit.mask(
                              mapped_fba.TargetFlowName.notnull(),
                              mapped_fba.TargetUnit),
                          FlowAmount=mapped_fba.FlowAmount.mask(
                              mapped_fba.TargetFlowName.notnull(),
                              mapped_fba.FlowAmount
                              * mapped_fba.ConversionFactor),
                          FlowUUID=mapped_fba.TargetFlowUUID
                      )
                      .drop(columns=mapping_fields))

        if any(mapped_fba.mapped == 'both'):
            log.info('Units standardized to %s by mapping to federal '
                     'elementary flow list', list(mapping.TargetUnit.unique()))
        if any(mapped_fba.mapped == 'left_only'):
            log.warning('Some units not standardized by mapping to federal '
                        'elementary flows list: %s',
                        list(mapped_fba
                             .query('mapped == "left_only"').Unit.unique()))

        return mapped_fba.drop(columns='mapped')

    # TODO: Can this be generalized to a _FlowBy method?
    def convert_to_geoscale(
        self: 'FlowByActivity',
        target_geoscale: Literal['national', 'state', 'county',
                                 geo.scale.NATIONAL, geo.scale.STATE,
                                 geo.scale.COUNTY]
    ) -> 'FlowByActivity':
        '''
        Converts, by filtering or aggregating (or both), the given dataset to
        the target geoscale.

        Rows from the calling FlowBy that correspond to a higher level (more
        aggregated) geoscale than the target are dropped. Then, for each
        combination of 'ActivityProducedBy' and 'ActivityConsumedBy', and for
        each level at or below (less aggregated than) the target geoscale,
        determine the highest level at which data is reported for each unit at
        that scale (so if the level is 'state', find the highest level at which
        data is reported for each state, for each activity combination).
        Finally, use this information to identify the correct source scale
        for each activity combination and regional unit (details below), then
        filter or aggregate (or both) to convert the dataset so all rows are
        at the target geoscale.

        For any region and activity combination, the correct source geoscale
        is the highest (most aggregated) geoscale at or below the target
        geoscale, for which data covering that region and activity combination
        is reported. For example, if the target geoscale is 'national',
        national level data should be used if available. If not, state level
        data should be aggregated up if available. However, if some states
        report county level data AND NOT state level data, then for those
        states (and only those states) county level data should be aggregated
        up. County level data from states that also report state level data
        should, in this example, be ignored.

        :param target_geoscale: str or geo.scale constant, the geoscale to
            convert the calling FlowBy data set to. Currently, this needs to be
            one which corresponds to a FIPS level (that is, one of national,
            state, or county)
        :return: FlowBy data set, with rows filtered or aggregated to the
            target geoscale.
        '''
        if type(target_geoscale) == str:
            target_geoscale = geo.scale.from_string(target_geoscale)

        log.info('Converting FBA to %s geoscale', target_geoscale.name.lower())

        geoscale_by_fips = pd.concat([
            (geo.filtered_fips(scale)
             .assign(geoscale=scale, National='USA')
             # ^^^ Need to have a column for each relevant scale
             # (only FIPS for now)
             .rename(columns={'FIPS': 'Location'}))
            for scale in [s for s in geo.scale if s.has_fips_level]
        ])

        geoscale_name_columns = [s.name.title() for s in geo.scale
                                 if s.has_fips_level]

        highest_reporting_level_by_geoscale = [
            (self
             .merge(geoscale_by_fips, how='inner')
             .query('geoscale <= @scale')
             .groupby(['ActivityProducedBy', 'ActivityConsumedBy']
                      + [s.name.title() for s in geo.scale
                         if s.has_fips_level and s >= scale],
                      dropna=False)
             .agg({'geoscale': 'max'})
             .reset_index()
             .rename(columns={
                 'geoscale': f'highest_reporting_level_by_{scale.name.title()}'
                 }))
            for scale in geo.scale
            if scale.has_fips_level and scale <= target_geoscale
        ]

        fba_with_reporting_levels = reduce(
            lambda x, y: x.merge(y, how='left'),
            [self, geoscale_by_fips, *highest_reporting_level_by_geoscale]
        )

        reporting_level_columns = [
            f'highest_reporting_level_by_{s.name.title()}'
            for s in geo.scale if s.has_fips_level and s <= target_geoscale
        ]

        fba_at_source_geoscale = (
            fba_with_reporting_levels
            .assign(source_geoscale=(
                fba_with_reporting_levels[reporting_level_columns]
                .max(axis='columns')))
            .query('geoscale == source_geoscale')
            .drop(columns=(['geoscale',
                            *geoscale_name_columns,
                            *reporting_level_columns]))
        )

        if len(fba_at_source_geoscale.source_geoscale.unique()) > 1:
            log.warning('FBA has multiple source geoscales: %s',
                        ', '.join([s.name.lower() for s in
                                   fba_at_source_geoscale
                                   .source_geoscale.unique()]))
        else:
            log.info('FBA source geoscale is %s',
                     fba_at_source_geoscale
                     .source_geoscale.unique()[0].name.lower())

        fba_at_target_geoscale = (
            fba_at_source_geoscale
            .drop(columns='source_geoscale')
            .update_fips_to_geoscale(target_geoscale)
            .aggregate_flowby()
        )

        return (
            fba_at_target_geoscale
            .astype({c: t for c, t
                     in flowby_config['fba_mapped_w_sector_fields'].items()
                     if c in fba_at_target_geoscale.columns})
            # ^^^ Need to convert back to correct dtypes after aggregating;
            #     otherwise, columns of NaN will become float dtype.
        )

    def map_to_sectors(
        self: 'FlowByActivity',
        industry_spec: dict,
        target_year: Literal[2002, 2007, 2012, 2017] = None,
        external_config_path: str = None
    ) -> 'FlowByActivity':
        '''
        Maps the activities in the calling dataframe to industries/sectors, but
        does not perform any attribution. Columns for SectorProducedBy and
        SectorConsumedBy are added to the FBA. Each activity may be matched
        with many industries/sectors, and each industry/sector may have many
        activities matched to it.

        The set of industries/sectors that activities are mapped to is
        determined by the industry_spec parameter. Currently, this is only
        able to be set in terms of NAICS codes, and specifies the desired
        default level of NAICS aggregation as well as different levels for
        specific groups of NAICS codes. See documentation for
        industries.naics_key_from_industry_spec for details on formatting
        the industry_spec dict.

        :param industry_spec: dict, formatted as in documentation for
            industries.naics_key_from_industry_spec. Gives the desired
            industry/sector aggregation level.
        :param target_year: int, which NAICS year to use.
        :param external_config_path: str, an external path to search for a
            crosswalk.
        '''
        if self.source_config['sector-like_activities']:
            log.info('Activities in %s are NAICS codes.',
                     self.source_name)
            try:
                source_year = int(self.source_config
                                  .get('activity_schema', '')[6:10])
            except ValueError:
                source_year = 2012
                log.warning('No NAICS year given for NAICS activities in %s. '
                            '2012 used as default.', self.source_name)
            else:
                log.info('NAICS Activities in %s use NAICS year %s.',
                         self.source_name, source_year)

            log.info('Converting NAICS codes to desired industry/sector '
                     'aggregation structure.')
            fba_w_naics = self.assign(
                ActivitySourceName=self.source_name,
                SectorType=np.nan
            )
            for direction in ['ProducedBy', 'ConsumedBy']:
                fba_w_naics = (
                    fba_w_naics
                    .merge(
                        industries.naics_key_from_industry_spec(industry_spec),
                        how='left',
                        left_on=f'Activity{direction}',
                        right_on='source_naics')
                    .rename(columns={'target_naics': f'Sector{direction}'})
                    .drop(columns='source_naics')
                )

        else:
            log.info('Getting crosswalk between activities in %s and '
                     'NAICS codes.', self.source_name)
            activity_to_source_naics_crosswalk = (
                sectormapping.get_activitytosector_mapping(
                    # ^^^ TODO: Replace or streamline get_...() function
                    (self.source_config.get('activity_to_sector_mapping')
                     or self.source_name),
                    fbsconfigpath=external_config_path)
                .astype('object')
                [['Activity', 'Sector', 'SectorType', 'SectorSourceName']]
            )

            source_years = list(
                (activity_to_source_naics_crosswalk.SectorSourceName
                 .str.removeprefix('NAICS_')
                 .str.removesuffix('_Code')
                 .dropna()
                 .astype('int').unique())
            )
            source_year = (2012 if 2012 in source_years
                           else max(source_years) if source_years
                           else 2012)
            if source_years:
                activity_to_source_naics_crosswalk = (
                    activity_to_source_naics_crosswalk
                    .query(f'SectorSourceName == "NAICS_{source_year}_Code"')
                    .reset_index(drop=True)
                )
            else:
                log.warning('No NAICS year/sector source name (e.g. '
                            '"NAICS_2012_Code") provided in crosswalk for %s. '
                            '2012 being used as default.',
                            self.source_name)

            log.info('Converting NAICS codes in crosswalk to desired '
                     'industry/sector aggregation structure.')
            activity_to_target_naics_crosswalk = (
                activity_to_source_naics_crosswalk
                .merge(industries.naics_key_from_industry_spec(industry_spec),
                       how='left', left_on='Sector', right_on='source_naics')
                .assign(Sector=lambda x: x.source_naics)
                .drop(columns=['source_naics', 'target_naics'])
                .drop_duplicates()
            )

            log.info('Mapping activities in %s to NAICS codes using crosswalk',
                     self.source_name)
            fba_w_naics = self
            for direction in ['ProducedBy', 'ConsumedBy']:
                fba_w_naics = (
                    fba_w_naics
                    .merge(activity_to_target_naics_crosswalk,
                           how='left',
                           left_on=f'Activity{direction}', right_on='Activity')
                    .rename(columns={'Sector': f'Sector{direction}',
                                     'SectorType': f'{direction}SectorType'})
                    .drop(columns=['ActivitySourceName',
                                   'SectorSourceName',
                                   'Activity'],
                          errors='ignore')
                )

        if source_year != target_year:
            log.info('Using NAICS time series/crosswalk to map NAICS codes '
                     'from NAICS year %s to NAICS year %s.',
                     source_year, target_year)
            naics_year_to_year_crosswalk = (
                pd.read_csv(
                    f'{settings.datapath}NAICS_Crosswalk_TimeSeries.csv',
                    dtype='object'
                )
                .assign(Sector=lambda x: x[f'NAICS_{source_year}_Code'],
                        NewSector=lambda x: x[f'NAICS_{target_year}_Code'])
                [['Sector', 'NewSector']]
                .drop_duplicates()
                .reset_index(drop=True)
            )
            for direction in ['ProducedBy', 'ConsumedBy']:
                fba_w_naics = (
                    fba_w_naics
                    .merge(naics_year_to_year_crosswalk,
                           how='left',
                           left_on=f'Sector{direction}', right_on='Sector')
                    .assign(**{f'Sector{direction}': lambda x: x['NewSector']})
                    .drop(columns=['Sector', 'NewSector'])
                )

        return (
            fba_w_naics
            .assign(SectorSourceName=f'NAICS_{target_year}_Code')
            .reset_index(drop=True)
        )

    def equal_attribution(self: 'FlowByActivity') -> 'FlowByActivity':
        '''
        This function takes a FlowByActivity dataset with SectorProducedBy and
        SectorConsumedBy columns already added and attributes flows from any
        activity which is mapped to multiple industries/sectors equally across
        those industries/sectors, by NAICS level. In other words, if an
        activity is mapped to multiple industries/sectors, the flow amount is
        equally divided across the relevant 2-digit NAICS industries. Then,
        within each 2-digit industry the flow amount for that industry is
        equally divided across the relevant 3-digit NAICS industries; within
        each of those, the flow amount is equally divided across relevant
        4-digit NAICS industries, and so on.

        For example:
        Suppose that activity A has a flow amount of 12 and is mapped to
        industries 111210, 111220, and 213110, a flow amount of 3 will be
        attributed to 111210, a flow amount of 3 to 111220, and a flow amount
        of 6 to 213110.

        Attribution happens according to the primary sector first (see
        documentation for
        flowby.FlowByActivity.add_primary_secondary_sector_columns() for
        details on how the primary sector is determined; in most cases, the
        primary sector is the (only) non-null value out of SectorProducedBy or
        SectorConsumedBy). If necessary, flow amounts are further (equally)
        subdivided based on the secondary sector.
        '''
        fba = self.add_primary_secondary_sectors()
        groupby_cols = [c for c in fba.columns if fba[c].dtype == 'object'
                        and c not in ['SectorProducedBy', 'SectorConsumedBy',
                                      'PrimarySector', 'SecondarySector',
                                      'Description']]

        for rank in ['Primary', 'Secondary']:
            fba = (
                fba
                .assign(
                    **{f'_naics_{n}': lambda x, i=n: x[f'{rank}Sector'].str[:i]
                        for n in range(2, 8)},
                    **{f'_unique_naics_{n}_by_group': lambda x, i=n: (
                            x
                            .groupby(groupby_cols if i == 2
                                     else [*groupby_cols, f'_naics_{i-1}'],
                                     dropna=False)
                            [[f'_naics_{i}']]
                            .transform('nunique', dropna=False)
                        )
                        for n in range(2, 8)},
                    FlowAmount=lambda x: reduce(
                        lambda x, y: x / y,
                        [x.FlowAmount, *[x[f'_unique_naics_{n}_by_group']
                                         for n in range(2, 8)]]
                    )
                )
            )
            groupby_cols.append('PrimarySector')

        return fba.drop(
            columns=['PrimarySector', 'SecondarySector',
                     *[f'_naics_{n}' for n in range(2, 8)],
                     *[f'_unique_naics_{n}_by_group' for n in range(2, 8)]]
        )


class FlowBySector(_FlowBy):
    _metadata = [*_FlowBy()._metadata]

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
        download_fbs_ok: bool = settings.DEFAULT_DOWNLOAD_IF_MISSING,
        **kwargs
    ) -> 'FlowBySector':
        '''
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
        :kwargs: keyword arguments to pass to _getFlowBy(). Possible kwargs
            include source_name and source_config.
        :return: FlowBySector dataframe
        '''
        file_metadata = metadata.set_fb_meta(method, 'FlowBySector')
        flowby_generator = partial(
            flowbysector.main,
            method=method,
            fbsconfigpath=external_config_path,
            download_FBAs_if_missing=download_sources_ok
        )
        return super()._getFlowBy(
            file_metadata=file_metadata,
            download_ok=download_fbs_ok,
            flowby_generator=flowby_generator,
            output_path=settings.fbsoutputpath,
            **kwargs
        )

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
        source_catalog = common.load_yaml_dict('source_catalog')

        source_fbs_list = []
        for source_name, source_config in sources.items():
            source_config = {
                **source_catalog.get(
                     common.return_true_source_catalog_name(source_name), {}),
                **source_config
            }

            if source_config['data_format'] in ['FBS', 'FBS_outside_flowsa']:
                if source_config['data_format'] == 'FBS_outside_flowsa':
                    source_data = FlowBySector(
                        source_config['FBS_datapull_fxn'](
                            source_config, method_config, external_config_path
                        ),
                        source_name=source_name,
                        source_config=source_config
                    )
                else:  # TODO: Test this section.
                    source_data = FlowBySector.getFlowBySector(
                        method=source_name,
                        external_config_path=external_config_path,
                        download_sources_ok=download_sources_ok,
                        download_fbs_ok=download_sources_ok,
                        source_name=source_name,
                        source_config=source_config
                    )

                fbs = (source_data
                       .conditional_pipe(
                           'clean_fbs_df_fxn' in source_config,
                           source_config.get('clean_fbs_df_fxn'))
                       .update_fips_to_geoscale(
                           method_config['target_geoscale']))

                if 'source_flows' in source_config:
                    source_flows = source_config['source_flows']
                    fbs = (fbs
                           .query('Flowable in @source_flows')
                           .conditional_method(isinstance(source_flows, dict),
                                               'replace',
                                               {'Flowable': source_flows})
                           .reset_index(drop=True))
                log.info('Appending %s to FBS list', source_name)
                source_fbs_list.append(fbs)

            if source_config['data_format'] == 'FBA':
                source_data = FlowByActivity.getFlowByActivity(
                    source_name=source_name,
                    year=source_config['year'],
                    download_ok=download_sources_ok,
                    source_config=source_config
                )

                fba = (source_data
                       .query(f'Class == \'{source_config["class"]}\'')
                       # TODO: source_config keys not currently handled:
                       # TODO: 'source_fba_load_scale', 'apply_urban_rural',
                       .conditional_pipe(
                           'clean_fba_before_mapping_df_fxn' in source_config,
                           source_config.get('clean_fba_before_mapping_df_fxn')
                           )
                       .map_to_fedefl_list()
                       .conditional_pipe(
                           'clean_fba_df_fxn' in source_config,
                           source_config.get('clean_fba_df_fxn')))

                activities = source_config['activity_sets']

                for activity_set, activity_config in activities.items():
                    log.info('Preparing to process %s in %s',
                             activity_set, source_name)

                    activity_names = activity_config['names']

                    activity_set_fba = (
                        fba
                        .query('ActivityProducedBy in @activity_names'
                               '| ActivityConsumedBy in @activity_names')
                        .reset_index(drop=True)
                    )

                    activity_set_fba.activity_set = activity_set
                    activity_set_fba.activity_config = activity_config

                    if 'source_flows' in activity_config:
                        source_flows = activity_config['source_flows']
                        activity_set_fba = (
                            activity_set_fba
                            .query('FlowName in @source_flows')
                            .conditional_method(isinstance(source_flows, dict),
                                                'replace',
                                                {'FlowName': source_flows})
                            .reset_index(drop=True)
                        )

                    if activity_set_fba.empty:
                        log.error('No data for flows in %s', activity_set)
                        continue
                    if (activity_set_fba.FlowAmount == 0).all():
                        log.warning('All flows for %s are 0', activity_set)
                        continue

                    # TODO: source_catalog key not currently handled:
                    # TODO: 'sector-like_activities'

                    assert isinstance(activity_set_fba, FlowByActivity)

                    activity_set_fba = (
                        activity_set_fba
                        .convert_to_geoscale(
                            max(geo.scale.from_string(
                                    source_config['geoscale_to_use']),
                                geo.scale.from_string(
                                    activity_config['allocation_from_scale'])))
                    )

                    if activity_config['allocation_from_scale'] != 'national':
                        validation.compare_geographic_totals(
                            activity_set_fba, fba, source_name,
                            activity_config, activity_set, activity_names
                            # ^^^ TODO: Rewrite validation to use fb metadata
                        )

                    assert isinstance(activity_set_fba, FlowByActivity)

                    activity_set_fba = (
                        activity_set_fba
                        .map_to_sectors(
                            industry_spec=method_config[
                                'target_industry_spec'],
                            target_year=method_config['target_naics_year'],
                            external_config_path=external_config_path
                        )
                        .conditional_pipe(
                            'clean_fba_w_sec_df-fxn' in source_config,
                            source_config.get('clean_fba_w_sec_df_fxn'),
                            attr=activity_config,
                            method=method_config
                        )
                        .rename(columns={'SourceName': 'MetaSources'})
                        .drop(columns=['FlowName', 'Compartment'])
                    )

                    assert isinstance(activity_set_fba, FlowByActivity)

                    if activity_config['allocation_method'] == 'direct':
                        log.info('Attributing flows in %s to sectors using '
                                 'direct attribution method', activity_set)
                        fbs = activity_set_fba.equal_attribution()
                    elif (activity_config['allocation_method']
                          == 'allocation_function'):
                        log.info(
                            'Attributing flows in %s to sectors using '
                            'function: %s.%s',
                            activity_set,
                            activity_config['allocation_source'].__module__,
                            activity_config['allocation_source'].__name__
                        )
                        fbs = activity_config['allocation_source'](
                            activity_set_fba,
                            activity_config,
                            source_fbs_list
                        )
                    elif (activity_config['allocation_method']
                          == 'proportional'):
                        pass

                    fba = fba.query(
                        'ActivityProducedBy not in @activity_names'
                        '& ActivityConsumedBy not in @activity_names'
                    )

                    log.info('Appending %s from %s to FBS list',
                             activity_set, source_name)
                    source_fbs_list.append(fbs)

        return source_fbs_list


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
