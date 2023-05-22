from typing import List, Literal, TypeVar, Union
import pandas as pd
from pandas import ExcelWriter
import numpy as np
from functools import partial, reduce
from copy import deepcopy
from flowsa import (common, settings, metadata, sectormapping,
                    literature_values, flowbyactivity, flowsa_yaml,
                    validation, geo, naics, exceptions, location)
from flowsa.flowsa_log import log, reset_log_file
import esupy.processed_data_mgmt
import esupy.dqi
import fedelemflowlist

FB = TypeVar('FB', bound='_FlowBy')
S = TypeVar('S', bound='_FlowBySeries')
NAME_SEP_CHAR = '.'
# ^^^ Used to separate source/activity set names as part of 'full_name' attr


with open(settings.datapath + 'flowby_config.yaml') as f:
    flowby_config = flowsa_yaml.load(f)
    # ^^^ Replaces schema.py


# TODO: Move this to common.py
def get_catalog_info(source_name: str) -> dict:
    '''
    Retrieves the information on a given source from source_catalog.yaml.
    Replaces (when used appropriately), common.check_activities_sector_like()
    as well as various pieces of code that load the source_catalog yaml.
    '''
    source_catalog = common.load_yaml_dict('source_catalog')
    source_name = common.return_true_source_catalog_name(source_name)
    return source_catalog.get(source_name, {})


# TODO: Should this be in the flowsa __init__.py?
def get_flowby_from_config(
    name: str,
    config: dict,
    external_config_path: str = None,
    download_sources_ok: bool = True
) -> FB:
    """
    Loads FBA or FBS dataframe from a config dictionary and attaches that
    dictionary to the FBA or FBS. Exists for convenience.

    :return: a FlowByActivity dataframe
    """
    external_data_path = config.get('external_data_path')

    if config['data_format'] == 'FBA':
        return FlowByActivity.getFlowByActivity(
            full_name=name,
            config=config,
            download_ok=download_sources_ok,
            external_data_path=external_data_path
        )
    elif config['data_format'] == 'FBS':
        return FlowBySector.getFlowBySector(
            method=name,
            config=config,
            external_config_path=external_config_path,
            download_sources_ok=download_sources_ok,
            download_fbs_ok=download_sources_ok,
            external_data_path=external_data_path
        )
    elif config['data_format'] == 'FBS_outside_flowsa':
        return FlowBySector(
            config['FBS_datapull_fxn'](
                config=config,
                external_config_path=external_config_path,
                full_name=name
            ),
            full_name=name,
            config=config
        )
    else:
        log.critical('Unrecognized data format %s for source %s',
                     config['data_format'], name)
        raise ValueError('Unrecognized data format')


class _FlowBy(pd.DataFrame):
    _metadata = ['full_name', 'config']

    full_name: str
    config: dict

    def __init__(
        self,
        data: pd.DataFrame or '_FlowBy' = None,
        *args,
        add_missing_columns: bool = True,
        fields: dict = None,
        column_order: List[str] = None,
        string_null: 'np.nan' or None = np.nan,
        **kwargs
    ) -> None:
        '''
        Extends pandas DataFrame. Attaches metadata if provided as kwargs and
        ensures that all columns described in  flowby_config.yaml are present
        and of the correct datatype.

        All args and kwargs not specified above or in FBA/FBS metadata are
        passed to the DataFrame constructor.
        '''

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
            if add_missing_columns:
                data = data.assign(**{field: None
                                      for field in fields
                                      if field not in data.columns})
            else:
                fields = {k: v for k, v in fields.items() if k in data.columns}

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
                    .fillna(fill_na_dict)
                    .replace(null_string_dict)
                    .astype(fields))

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

        merge: use metadata of left FlowBy
        concat: for full_name or config, use the shared portion (possibly
            '' or {}); for other _metadata (if any), use values from the
            first FlowBy
        '''
        self = super().__finalize__(other, method=method, **kwargs)

        # When merging, use metadata from left FlowBy
        if method == 'merge':
            for attribute in self._metadata:
                object.__setattr__(self, attribute,
                                   getattr(other.left, attribute, None))

        # When concatenating, use shared portion of full_name or config. For
        # other attributes, use metadata from the first FlowBy
        if method == 'concat':
            _name_list = []
            for i, n in enumerate(getattr(other.objs[0], 'full_name', '')
                                  .split(NAME_SEP_CHAR)):
                if all(n == getattr(x, 'full_name', '').split(NAME_SEP_CHAR)[i]
                       for x in other.objs[1:]):
                    _name_list.append(n)
                else:
                    break
            _full_name = NAME_SEP_CHAR.join(_name_list)

            _config = {
                k: v for k, v in getattr(other.objs[0], 'config', {}).items()
                if all(v == getattr(x, 'config', {}).get(k)
                       for x in other.objs[1:])
            }
            object.__setattr__(self, 'full_name', _full_name)
            object.__setattr__(self, 'config', _config)
            for attribute in [x for x in self._metadata
                              if x not in ['full_name', 'config']]:
                object.__setattr__(self, attribute,
                                   getattr(other.objs[0], attribute, None))
        return self

    @property
    def source_name(self) -> str:
        return self.full_name.split('.', maxsplit=1)[0]

    @property
    def flow_col(self) -> str:
        return 'Flowable' if 'Flowable' in self else 'FlowName'

    @property
    def groupby_cols(self) -> List[str]:
        return [x for x in self
                if self[x].dtype in ['int', 'object'] and x != 'Description']

    @classmethod
    def _getFlowBy(
        cls,
        file_metadata: esupy.processed_data_mgmt.FileMeta,
        download_ok: bool,
        flowby_generator: partial,
        output_path: str,
        *,
        full_name: str = None,
        config: dict = None,
        external_data_path: str = None
    ) -> '_FlowBy':
        paths = deepcopy(settings.paths)
        paths.local_path = external_data_path or paths.local_path

        attempt_list = (['import local', 'download', 'generate']
                        if download_ok else ['import local', 'generate'])

        for attempt in attempt_list:
            log.info(
                'Attempting to %s %s %s',
                attempt, file_metadata.name_data, file_metadata.category
            )
            if attempt == 'download':
                esupy.processed_data_mgmt.download_from_remote(
                    file_metadata,
                    paths
                )
            if attempt == 'generate':
                flowby_generator()
            df = esupy.processed_data_mgmt.load_preprocessed_output(
                file_metadata,
                paths
            )
            if df is None:
                log.info(
                    '%s %s not found in %s',
                    file_metadata.name_data,
                    file_metadata.category,
                    paths.local_path
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
        fb = cls(df, full_name=full_name or '', config=config or {})
        return fb

    def convert_daily_to_annual(self: FB) -> FB:
        daily_list = ['/d', '/day']
        if any(self.Unit.str.endswith(tuple(daily_list))):
            log.info('Converting daily flows %s to annual',
                     [unit for unit in self.Unit.unique() if any(
                         x in unit for x in daily_list)])
        return (
            self
            .assign(FlowAmount=self.FlowAmount.mask(
                self.Unit.str.endswith(tuple(daily_list)),
                self.FlowAmount * 365),
                Unit=self.Unit.apply(lambda x: x.split('/d', 1)[0]))
        )

    def standardize_units(self: FB, year: int = None) -> FB:
        exchange_rate = (
            literature_values
            .get_Canadian_to_USD_exchange_rate(year or self.config['year'])
        )

        conversion_table = pd.concat([
            pd.read_csv(f'{settings.datapath}unit_conversion.csv'),
            pd.Series({'old_unit': 'Canadian Dollar',
                       'new_unit': 'USD',
                       'conversion_factor': 1 / exchange_rate}).to_frame().T
        ])

        standardized = (
            self
            .assign(Unit=self.Unit.str.strip())
            .merge(conversion_table, how='left',
                   left_on='Unit', right_on='old_unit')
            .assign(Unit=lambda x: x.new_unit.mask(x.new_unit.isna(), x.Unit),
                    conversion_factor=lambda x: x.conversion_factor.fillna(1),
                    FlowAmount=lambda x: x.FlowAmount * x.conversion_factor)
            .drop(columns=['old_unit', 'new_unit', 'conversion_factor'])
        )

        standardized_units = list(conversion_table.new_unit.unique())

        if any(~standardized.Unit.isin(standardized_units)):
            unstandardized_units = [unit for unit in standardized.Unit.unique()
                                    if unit not in standardized_units]
            log.warning(f'Some units in {standardized.full_name} not '
                        f'standardized by standardize_units(): '
                        f'{unstandardized_units}.')

        return standardized

    def function_socket(
        self: FB,
        socket_name: str,
        *args, **kwargs
    ) -> FB:
        '''
        Allows us to define positions ("sockets") in method chains where a user
        defined function can be applied to the FlowBy. Such functions should
        take as their first argument a FlowBy, and return a FlowBy. Most of the
        existing functions in various source.py files that are used in this way
        already work, since they take, and return, DataFrames. If passed a
        FlowBy, they generally therefore return a FlowBy.

        :param socket_name: str, the key in self.config where the function (or
            list of functions) to be applied is found.
        :param *args, **kwargs: passed indiscriminately to the function or
            functions specified in self.config[socket_name].
        :return: transformed FlowBy dataset
        '''
        if socket_name in self.config:
            if isinstance(self.config[socket_name], list):
                return reduce(lambda fb, func: fb.pipe(func, *args, **kwargs),
                              self.config[socket_name],
                              self)
            else:
                return self.config[socket_name](self, *args, **kwargs)
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

    def convert_fips_to_geoscale(
        self: FB,
        target_geoscale: Literal['national', 'state', 'county',
                                 geo.scale.NATIONAL, geo.scale.STATE,
                                 geo.scale.COUNTY] = None,
        column: str = 'Location'
    ) -> FB:
        """
        Sets FIPS codes to 5 digits by zero-padding FIPS codes at the specified
        geoscale on the right (county geocodes are unmodified, state codes are
        generally padded with 3 zeros, and the "national" FIPS code is set to
        00000)
        :param to_geoscale: str, target geoscale
        :param column: str, column of FIPS codes to convert.
            Default = 'Location'
        :return: FlowBy dataset with 5 digit fips
        """
        target_geoscale = target_geoscale or self.config.get('geoscale')
        if type(target_geoscale) == str:
            target_geoscale = geo.scale.from_string(target_geoscale)

        if target_geoscale == geo.scale.NATIONAL:
            return self.assign(
                **{column: geo.filtered_fips('national').FIPS.values[0]}
            )
        elif target_geoscale == geo.scale.STATE:
            return self.assign(
                **{column: self[column].str.slice_replace(start=2, repl='000')}
            )
        elif target_geoscale == geo.scale.COUNTY:
            return self
        else:
            log.error(f'No FIPS level corresponds to {target_geoscale}')

    def select_by_fields(
        self: FB,
        selection_fields: dict = None,
        exclusion_fields: dict = None
    ) -> FB:
        '''
        Filter the calling FlowBy dataset according to the 'selection_fields'
        dictionary from the calling datasets config dictionary. If such a
        dictionary is not given, the calling dataset is returned unchanged.

        The selection_fields dictionary should associate FBA or FBS
        fields/column names with lists of the values to be selected from
        each column. For example:

        selection_fields:
          FlowName:
            - CO2
            - CH4

        If only a single value is to be selected from a given column, it may
        be given as a string scalar value instead of a single-element list:

        selection_fields:
          FlowName: CO2

        Alternatively, instead of a list of values, a dictionary may be
        given which associates the values to select with a replacement value:

        selection_fields:
          FlowName:
            CO2: Carbon Dioxide
            CH4: Methane

        Finally, if the selection_fields dictionary contains the keys
        'Activity' or 'Sector', rows which contain the given values in either
        the relevant ...ProducedBy or ...ConsumedBy columns will be selected.
        Alternatively, if the selection_fields dictionary contains the keys
        'PrimaryActivity' or 'PrimarySector', the relevant column(s) will be
        added using _FlowBy.add_primary_secondary_columns(), and the selection
        made based on 'PrimaryActivity' or 'PrimarySector', as specified.
        Selecting on secondary activities or sectors is not supported.

        Similarly, can use 'exclusion_fields' to remove particular data in the
        same manner.
        '''
        exclusion_fields = (exclusion_fields or
                            self.config.get('exclusion_fields', {}))
        exclusion_fields = {k: [v] if not isinstance(v, (list, dict)) else v
                            for k, v in exclusion_fields.items()}
        for field, values in exclusion_fields.items():
            if field == 'conditional':
                qry = ' & '.join(
                    ["({} in {})".format(
                        k, [v] if not isinstance(v, (list,dict)) else v)
                        for k, v in exclusion_fields['conditional'].items()]
                    )
                self = self.query(f'~({qry})')
            else:
                if field not in self:
                    log.warning(f'{field} not found, can not apply '
                                'exclusion_fields')
                else:
                    self = self.query(f'{field} not in @values')

        selection_fields = (selection_fields
                            or self.config.get('selection_fields'))

        if selection_fields is None:
            return self

        selection_fields = {k: [v] if not isinstance(v, (list, dict)) else v
                            for k, v in selection_fields.items()}

        if 'PrimaryActivity' in selection_fields:
            self = (self
                    .add_primary_secondary_columns('Activity')
                    .drop(columns='SecondaryActivity'))
        if 'PrimarySector' in selection_fields:
            self = (self
                    .add_primary_secondary_columns('Sector')
                    .drop(columns='SecondarySector'))

        special_fields = {
            k: v for k, v in selection_fields.items()
            if k in ['Activity', 'Sector']
        }
        other_fields = {
            k: v for k, v in selection_fields.items()
            if k not in ['Activity', 'Sector']
        }

        filtered_fb = self
        for field, values in special_fields.items():
            check_values = ([*values.keys(), *values.values()]
                            if isinstance(values, dict) else values)
            filtered_fb = filtered_fb.query(
                f'{field}ProducedBy in @check_values '
                f'| {field}ConsumedBy in @check_values'
            )
        for field, values in other_fields.items():
            check_values = ([*values.keys(), *values.values()]
                            if isinstance(values, dict) else values)
            filtered_fb = filtered_fb.query(f'{field} in @check_values')

        if filtered_fb.empty:
            log.warning('%s FBA is empty', filtered_fb.full_name)

        for k in ['Activity', 'Sector']:
            if isinstance(other_fields.get(f'Primary{k}'), dict):
                if isinstance(special_fields.get(k), dict):
                    special_fields[k].update(other_fields.pop(f'Primary{k}'))
                else:
                    special_fields[k] = other_fields.pop(f'Primary{k}')

        replace_dict = {
            **{f'{k}ProducedBy': v for k, v in special_fields.items()
               if isinstance(v, dict)},
            **{f'{k}ConsumedBy': v for k, v in special_fields.items()
               if isinstance(v, dict)},
            **{k: v for k, v in other_fields.items()
               if isinstance(v, dict)}
        }

        replaced_fb = (
            filtered_fb
            .replace(replace_dict)
            .drop(columns=['PrimaryActivity', 'PrimarySector'],
                  errors='ignore')
            .reset_index(drop=True)
        )
        return replaced_fb

    def aggregate_flowby(
            self: FB,
            columns_to_group_by: List[str] = None,
            columns_to_average: List[str] = None,
            retain_zeros: bool = False
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
            columns_to_group_by = self.groupby_cols
        if columns_to_average is None:
            columns_to_average = [
                x for x in self.columns
                if self[x].dtype == 'float' and x != 'FlowAmount'
            ]

        if not retain_zeros:
            self = self.query('FlowAmount != 0')
            # ^^^ keep rows of zero values

        fb = self.drop(columns=[c for c in self.columns
                                if c not in ['FlowAmount',
                                             *columns_to_average,
                                             *columns_to_group_by]])

        aggregated = (
            fb
            .assign(**{f'_{c}_weighted': fb[c] * fb.FlowAmount
                    for c in columns_to_average},
                    **{f'_{c}_weights': fb.FlowAmount * fb[c].notnull()
                    for c in columns_to_average})
            .groupby(columns_to_group_by, dropna=False)
            .agg(sum)
            .reset_index()
        )
        aggregated = (
            aggregated
            .assign(**{c: (aggregated[f'_{c}_weighted']
                           / aggregated[f'_{c}_weights'])
                       for c in columns_to_average})
            .drop(columns=([*[f'_{c}_weighted' for c in columns_to_average],
                            *[f'_{c}_weights' for c in columns_to_average]]))
        )
        aggregated = aggregated.astype(
            {column: type for column, type
             in set([*flowby_config['all_fba_fields'].items(),
                     *flowby_config['all_fbs_fields'].items()])
             if column in aggregated}
        )
        # ^^^ Need to convert back to correct dtypes after aggregating;
        #     otherwise, columns of NaN will become float dtype.
        return aggregated

    def attribute_flows_to_sectors(
        self: FB,
        external_config_path: str = None
    ) -> FB:
        """
        The calling FBA has its activities mapped to sectors, then its flows
        attributed to those sectors, by the methods specified in the calling
        FBA's configuration dictionary.
        """
        attribute_config = self.config.get('attribute', None)

        if isinstance(attribute_config, dict):
            attribute_config = [attribute_config]
        if attribute_config is None:
            log.error('Attribution method is missing')

        for step_config in attribute_config:
            grouped: 'FB' = (
                self
                .reset_index(drop=True).reset_index()
                .rename(columns={'index': 'group_id'})
                .assign(group_total=self.FlowAmount)
            )
            if len(grouped)==0:
                log.warning(f'No data remaining in {self.full_name}.')
                return self
            if self.config['data_format'] == 'FBA':
                fb: 'FlowByActivity' = (
                    grouped
                    .map_to_sectors(external_config_path=external_config_path)
                    .function_socket('clean_fba_w_sec',
                                     attr=self.config,
                                     method=self.config)
                    .rename(columns={'SourceName': 'MetaSources'})
                )
            elif self.config['data_format'] == 'FBS':
                fb = grouped.copy()

            attribution_method = step_config.get('attribution_method')
            if 'attribution_source' in step_config:
                for k, v in step_config['attribution_source'].items():
                    attribution_name = k

            if attribution_method == 'direct':
                log.info(f"Directly attributing {self.full_name} to "
                         f"target sectors.")
                fb = fb.assign(AttributionSources='Direct')
            else:
                fb = fb.assign(AttributionSources=','.join(
                    [k for k in step_config.get('attribution_source').keys()]))

            if attribution_method == 'proportional':
                log.info(f"Proportionally attributing {self.full_name} to "
                         f"target sectors with {attribution_name}")
                attribution_fbs = fb.load_prepare_attribution_source(
                    attribution_config=step_config
                )
                attributed_fb = fb.proportionally_attribute(attribution_fbs)

            elif attribution_method == 'multiplication':
                log.info(f"Multiplying {self.full_name} by {attribution_name}")
                attribution_fbs = fb.load_prepare_attribution_source(
                    attribution_config=step_config
                )
                attributed_fb = fb.multiplication_attribution(attribution_fbs)

            else:
                if all(fb.groupby('group_id')['group_id'].agg('count') == 1):
                    log.info('No attribution needed for %s at the given industry '
                             'aggregation level', fb.full_name)
                    return fb.drop(columns=['group_id', 'group_total'])

                if attribution_method is None:
                    log.warning('No attribution method specified for %s. '
                                'Using equal attribution as default.',
                                fb.full_name)
                elif attribution_method != 'direct':
                    log.error('Attribution method for %s not recognized: %s',
                              fb.full_name, attribution_method)
                    raise ValueError('Attribution method not recognized')

                attributed_fb = fb.equally_attribute()

            # if the attribution method is not multiplication, check that new df
            # values equal original df values
            if attribution_method not in ['multiplication', 'weighted_average',
                                          'substitute_nonexistent_values']:
                # todo: add results from this if statement to validation log
                validation_fb = attributed_fb.assign(
                    validation_total=(attributed_fb.groupby('group_id')
                                      ['FlowAmount'].transform('sum'))
                )
                if not np.allclose(validation_fb.group_total,
                                   validation_fb.validation_total,
                                   equal_nan=True):
                    errors = (validation_fb
                              .query('validation_total != group_total')
                              [['group_id',
                                'ActivityProducedBy', 'ActivityConsumedBy',
                                'SectorProducedBy', 'SectorConsumedBy',
                                'FlowAmount', 'group_total', 'validation_total']])
                    log.error('Errors in attributing flows from %s:\n%s',
                              self.full_name, errors)

        return attributed_fb.drop(columns=['group_id', 'group_total'])


    def activity_sets(self) -> List['FB']:
        '''
        This function breaks up an FBA dataset into its activity sets, if its
        config dictionary specifies activity sets, and returns a list of the
        resulting FBAs. Otherwise, it returns a list containing the calling
        FBA.

        Activity sets are determined by the selection_field key under each
        activity set name. An error will be logged if any rows from the calling
        FBA are assigned to multiple activity sets.
        '''
        if 'activity_sets' not in self.config:
            return [self]

        log.info('Splitting %s into activity sets', self.full_name)
        activities = self.config['activity_sets']
        parent_config = {k: v for k, v in self.config.items()
                         if k not in ['activity_sets',
                                      'clean_fba_before_activity_sets']
                         and not k.startswith('_')}
        parent_df = self.reset_index().rename(columns={'index': 'row'})

        child_df_list = []
        assigned_rows = set()
        for activity_set, activity_config in activities.items():
            log.info('Creating subset for %s', activity_set)

            child_df = (
                parent_df
                .add_full_name(
                    f'{parent_df.full_name}{NAME_SEP_CHAR}{activity_set}')
                .select_by_fields(
                    selection_fields=activity_config.get('selection_fields'),
                    exclusion_fields=activity_config.get('exclusion_fields'))
            )

            child_df.config = {**parent_config, **activity_config}
            child_df = child_df.assign(SourceName=child_df.full_name)

            if set(child_df.row) & assigned_rows:
                log.critical(
                    'Some rows from %s assigned to multiple activity '
                    'sets. This will lead to double-counting:\n%s',
                    parent_df.full_name,
                    child_df.query(
                        f'row in {list(set(child_df.row) & assigned_rows)}'
                    )
                )
                # raise ValueError('Some rows in multiple activity sets')

            assigned_rows.update(child_df.row)
            if not child_df.empty:
                child_df_list.append(child_df.drop(columns='row'))
            else:
                log.error('Activity set %s is empty. Check activity set '
                          'definition!', child_df.full_name)

        if set(parent_df.row) - assigned_rows:
            log.warning('Some rows from %s not assigned to an activity '
                        'set. Is this intentional?', parent_df.full_name)
            unassigned = parent_df.query('row not in @assigned_rows')

        return child_df_list

    def load_prepare_attribution_source(
        self: 'FlowByActivity',
        attribution_config=None
    ) -> 'FlowBySector':

        if attribution_config is None:
            attribution_source = self.config['attribute']['attribution_source']
        else:
            attribution_source = attribution_config['attribution_source']

        if isinstance(attribution_source, str):
            name, config = attribution_source, {}
        else:
            (name, config), = attribution_source.items()

        if name in self.config['cache']:
            attribution_fbs = self.config['cache'][name].copy()
            attribution_fbs.config = {
                **{k: attribution_fbs.config[k]
                   for k in attribution_fbs.config['method_config_keys']},
                **config
            }
            attribution_fbs = attribution_fbs.prepare_fbs()
        else:
            attribution_fbs = get_flowby_from_config(
                name=name,
                config={**{k: v for k, v in self.config.items()
                           if k in self.config['method_config_keys']
                           or k == 'method_config_keys'},
                        **get_catalog_info(name),
                        **config}
            ).prepare_fbs()
        return attribution_fbs


    def proportionally_attribute(
        self: 'FB',  # flowbyactivity or flowbysector
        other: 'FlowBySector'
    ) -> 'FlowByActivity':
        '''
        This method takes flows from the calling FBA which are mapped to
        multiple sectors and attributes them to those sectors proportionally to
        flows from other (an FBS).
        '''

        log.info('Attributing flows in %s using %s.',
                 self.full_name, other.full_name)

        fba_geoscale, other_geoscale, fba, other = self.harmonize_geoscale(other)

        groupby_cols = ['group_id']
        for rank in ['Primary', 'Secondary']:
            # skip over Secondary if not relevant
            if fba[f'{rank}Sector'].isna().all():
                continue
            counted = fba.assign(group_count=(fba.groupby(groupby_cols)
                                              ['group_id']
                                              .transform('count')))
            directly_attributed = (
                counted
                .query('group_count == 1')
                .drop(columns='group_count')
            )
            needs_attribution = (
                counted
                .query('group_count > 1')
                .drop(columns='group_count')
            )

            merged = (
                needs_attribution
                .merge(other,
                       how='left',
                       left_on=[f'{rank}Sector',
                                'temp_location'
                                if 'temp_location' in needs_attribution
                                else 'Location'],
                       right_on=['PrimarySector', 'Location'],
                       suffixes=[None, '_other'])
                .fillna({'FlowAmount_other': 0})
            )

            denominator_flag = ~merged.duplicated(subset=[*groupby_cols,
                                                  f'{rank}Sector'])
            with_denominator = (
                merged
                .assign(denominator=(
                    merged
                    .assign(FlowAmount_other=(merged.FlowAmount_other
                                              * denominator_flag))
                    .groupby(groupby_cols)
                    ['FlowAmount_other']
                    .transform('sum')))
            )

            non_zero_denominator = with_denominator.query(f'denominator != 0 ')
            unattributable = with_denominator.query(f'denominator == 0 ')

            if not unattributable.empty:
                log.warning(
                    'Could not attribute activities in %s due to lack of '
                    'flows in attribution source %s for mapped %s sectors %s',
                    # set(zip(unattributable.ActivityProducedBy,
                    #         unattributable.ActivityConsumedBy,
                    #         unattributable.Location)),
                    unattributable.full_name,
                    other.full_name,
                    rank,
                    sorted(set(unattributable[f'{rank}Sector']))
                )

            proportionally_attributed = (
                non_zero_denominator
                .assign(FlowAmount=lambda x: (x.FlowAmount
                                              * x.FlowAmount_other
                                              / x.denominator))
                .drop(columns=['PrimarySector_other', 'Location_other',
                               'FlowAmount_other', 'denominator',
                               'Unit_other'],
                      errors='ignore')
            )
            fba = pd.concat([directly_attributed,
                             proportionally_attributed], ignore_index=True)
            groupby_cols.append(f'{rank}Sector')

        return (
            fba
            .drop(columns=['PrimarySector', 'SecondarySector',
                           'temp_location'],
                  errors='ignore')
            .reset_index(drop=True)
        )

    def harmonize_geoscale(
        self: 'FB',
        other: 'FlowBySector'
    ) -> 'FlowByActivity':

        fba_geoscale = geo.scale.from_string(self.config['geoscale'])
        other_geoscale = geo.scale.from_string(other.config['geoscale'])

        if other_geoscale < fba_geoscale:
            log.info('Aggregating %s from %s to %s', other.full_name,
                     other_geoscale, fba_geoscale)
            other = (
                other
                .convert_fips_to_geoscale(fba_geoscale)
                .aggregate_flowby()
            )
        elif other_geoscale > fba_geoscale:
            log.info('%s is %s, while %s is %s, so attributing %s to '
                     '%s', other.full_name, other_geoscale, self.full_name,
                     fba_geoscale, other_geoscale, fba_geoscale)
            self = (
                self
                .assign(temp_location=self.Location)
                .convert_fips_to_geoscale(other_geoscale,
                                          column='temp_location')
            )

        fba = self.add_primary_secondary_columns('Sector')
        other = (
            other
            .add_primary_secondary_columns('Sector')
            [['PrimarySector', 'Location', 'FlowAmount', 'Unit']]
            .groupby(['PrimarySector', 'Location', 'Unit'])
            .agg('sum')
            .reset_index()
        )

        return fba_geoscale, other_geoscale, fba, other


    def multiplication_attribution(
        self: 'FB',
        other: 'FlowBySector'
    ) -> 'FlowByActivity':
        """
        This method takes flows from the calling FBA which are mapped to
        multiple sectors and multiplies them by flows from other (an FBS).
        """

        log.info('Multiplying flows in %s by %s.',
                 self.full_name, other.full_name)
        fba_geoscale, other_geoscale, fba, other = self.harmonize_geoscale(
            other)

        # todo: update units after multiplying

        # multiply using each dfs primary sector col
        merged = (fba
                  .merge(other,
                         how='left',
                         left_on=['PrimarySector', 'temp_location'
                                  if 'temp_location' in fba
                                  else 'Location'],
                         right_on=['PrimarySector', 'Location'],
                         suffixes=[None, '_other'])
                  .fillna({'FlowAmount_other': 0})
                  )

        fba = (merged
               .assign(FlowAmount=lambda x: (x.FlowAmount
                                             * x.FlowAmount_other))
               .drop(columns=['PrimarySector_other', 'Location_other',
                              'FlowAmount_other', 'denominator'],
                     errors='ignore')
               )

        # determine if any flows are lost because multiplied by 0
        fba_null = fba[fba['FlowAmount'] == 0]
        if len(fba_null) > 0:
            log.warning('FlowAmounts in %s are reset to 0 due to lack of '
                        'flows in attribution source %s for '
                        'ActivityProducedBy/ActivityConsumedBy/Location: %s',
                        fba.full_name, other.full_name,
                        set(zip(fba_null.ActivityProducedBy,
                                fba_null.ActivityConsumedBy,
                                fba_null.Location))
                        )

        return (
            fba
            .drop(columns=['PrimarySector', 'SecondarySector',
                           'temp_location'],
                  errors='ignore')
            .reset_index(drop=True)
        )

    def add_primary_secondary_columns(
        self: FB,
        col_type: Literal['Activity', 'Sector']
    ) -> FB:
        '''
        This function adds to the calling dataframe 'Primary...' and
        'Secondary...' columns (where ... is specified by col_type, as either
        'Activity' or 'Sector') based on the '...ProducedBy' and
        '...ConsumedBy' columns, and logic based on the type of flow. The
        original dataset is returned unchanged if it lacks '...ProducedBy'
        or '...ConsumedBy' columns.

        If the flow type is TECHNOSPHERE_FLOW, the primary sector or activity
        is ...ConsumedBy. Otherwise, it is ...ProducedBy unless only
        ...ConsumedBy is given, or if both are given and ...ProducedBy
        is one of 22, 221, 2213, 22131, or 221310 AND ...ConsumedBy is one
        of F010, F0100, or F01000.

        In all cases, the secondary sector or activity is the other one if both
        are given. In many cases, only one of ...ProducedBy or ...ConsumedBy
        is given, and therefore Secondary... is null.

        :param col_type: str, one of 'Activity' or 'Sector', specifies whether
            primary and secondary activities or primary and secondary sectors
            should be added.
        :return: FlowBy dataset, with 'Primary...' and 'Secondary...'
            columns added, if possible; otherwise, the unmodified caling FlowBy
            dataset.
        '''
        if (f'{col_type}ProducedBy' not in self
                or f'{col_type}ConsumedBy' not in self):
            log.error(f'Cannot add Primary{col_type} or Secondary{col_type} '
                      f'columns, since {col_type}ProducedBy and/or '
                      f'{col_type}ConsumedBy columns are missing.')
            return self

        else:
            if 'primary_action_type' in self.config:
                primary_action_type = self.config['primary_action_type']
                secondary_action_type = ('Consumed'
                                         if primary_action_type == 'Produced'
                                         else 'Produced')

                fb = self.assign(
                    **{f'Primary{col_type}':
                        self[f'{col_type}{primary_action_type}By'],
                       f'Secondary{col_type}':
                        self[f'{col_type}{secondary_action_type}By']}
                )

            else:
                fb = self.assign(
                    **{f'Primary{col_type}':
                        self[f'{col_type}ProducedBy'].mask(
                            ((self.FlowType == 'TECHNOSPHERE_FLOW')
                             | (self[f'{col_type}ProducedBy'].isna())
                             | (self[f'{col_type}ProducedBy'].isin(
                                 ['22', '221', '2213', '22131', '221310']
                             )
                             & self[f'{col_type}ConsumedBy'].isin(
                                 ['F010', 'F0100', 'F01000']
                             )))
                            & self[f'{col_type}ConsumedBy'].notna(),
                            self[f'{col_type}ConsumedBy']
                        )
                       }
                )

                def _identify_secondary(row: _FlowBySeries) -> str:
                    sectors = [row[f'{col_type}ProducedBy'],
                               row[f'{col_type}ConsumedBy']]
                    sectors.remove(row[f'Primary{col_type}'])
                    return sectors[0]

                fb = fb.assign(
                    **{f'Secondary{col_type}': (
                        fb.apply(_identify_secondary, axis='columns')
                        # ^^^ Applying with axis='columns' applies TO each row.
                        .astype('object'))}
                )

            return fb

    def add_full_name(self: FB, full_name: str) -> FB:
        fb = self.copy()
        fb.full_name = full_name
        return fb

    def to_parquet(self: FB, *args, **kwargs) -> None:
        pd.DataFrame(self).to_parquet(*args, **kwargs)
        # ^^^ For some reason, the extra features of a FlowBy stop the
        #     to_parquet method inherited from DatFrame from working, so this
        #     casts the data back to plain DataFrame to write to a parquet.

    def astype(self: FB, *args, **kwargs) -> FB:
        '''
        Overrides DataFrame.astype(). Necessary only for pandas >= 1.5.0.
        With this update, DataFrame.astype() calls the constructor method
        of the calling dataframe. The FlowBy constructor 1) calls .astype() and
        2) adds missing FlowBy columns. Consequently, when it's called in the
        middle of DataFrame.astype() it creates an infinite loop. When the
        infinite loop is fixed elsewhere, a problem still exists because of
        missing columns being added (the data submitted to the constructor by
        DataFrame.astype() don't have column names, so a bunch of empty columns
        are added on the end and then .astype() can't assign data types and add
        column names back properly). This function fixes the problem by making
        it so DataFrame.astype() is not called by a FlowBy dataframe, but
        instead by a plain pd.DataFrame.
        '''
        metadata = {attribute: self.__getattr__(attribute)
                    for attribute in self._metadata}
        df = pd.DataFrame(self).astype(*args, **kwargs)
        fb = type(self)(df, add_missing_columns=False, **metadata)

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
        full_name: str,
        year: int = None,
        config: dict = None,
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
            include config.
        :return: a FlowByActivity dataframe
        """
        if year is None and isinstance(config, dict):
            year = config.get('year')

        file_metadata = metadata.set_fb_meta(
            full_name if year is None else f'{full_name}_{year}',
            'FlowByActivity'
        )
        flowby_generator = partial(
            flowbyactivity.main,
            source=full_name,
            year=year
        )
        return super()._getFlowBy(
            file_metadata=file_metadata,
            download_ok=download_ok,
            flowby_generator=flowby_generator,
            output_path=settings.fbaoutputpath,
            full_name=full_name,
            config=config,
            **kwargs
        )

    # TODO: probably only slight modification is needed to allow for material
    # flow list mapping using this function as well.
    def map_to_fedefl_list(
        self: 'FlowByActivity',
        drop_unmapped_rows: bool = False
    ) -> 'FlowByActivity':
        fba_merge_keys = [
            'Flowable',
            'Unit',
            'Context'
        ]
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
            'SourceFlowName',
            'SourceUnit',
            'SourceFlowContext'
        ]
        merge_type = 'inner' if drop_unmapped_rows else 'left'

        mapping_subset = self.config.get('fedefl_mapping')

        log.info(f'Mapping flows in {self.full_name} to '
                 f'{mapping_subset} in federal elementary flow list')

        # Check for use of multiple mapping files
        # TODO this was handled in esupy originally - can we go back to that fxn?
        if isinstance(mapping_subset, list):
            fba_merge_keys.append('SourceName')
            mapping_merge_keys.append('SourceListName')

        fba = (
            self
            .assign(Flowable=self.FlowName,
                    Context=self.Compartment,
                    )
            .drop(columns=['FlowName', 'Compartment'])
        )

        mapping = (
            fedelemflowlist.get_flowmapping(mapping_subset)[mapping_fields]
            .assign(ConversionFactor=lambda x: x.ConversionFactor.fillna(1))
        )
        if mapping.empty:
            log.error('Elementary flow list entries for %s not found',
                      mapping_subset)
            return FlowByActivity(self, mapped=True)

        mapped_fba = fba.merge(mapping,
                               how=merge_type,
                               left_on=fba_merge_keys,
                               right_on=mapping_merge_keys,
                               indicator='mapped')

        is_mappable = mapped_fba.TargetFlowName.notnull()
        mapped_fba = (
            mapped_fba
            .assign(
                Flowable=mapped_fba.Flowable.mask(
                    is_mappable, mapped_fba.TargetFlowName),
                Context=mapped_fba.Context.mask(
                    is_mappable, mapped_fba.TargetFlowContext),
                Unit=mapped_fba.Unit.mask(
                    is_mappable, mapped_fba.TargetUnit),
                FlowAmount=mapped_fba.FlowAmount.mask(
                    is_mappable,
                    mapped_fba.FlowAmount * mapped_fba.ConversionFactor),
                FlowUUID=mapped_fba.TargetFlowUUID
            )
            .drop(columns=mapping_fields)
        )

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
                                 geo.scale.COUNTY] = None
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
        if self.LocationSystem.eq('Census_Region').all():
            return self
        target_geoscale = target_geoscale or self.config.get('geoscale')
        if type(target_geoscale) == str:
            target_geoscale = geo.scale.from_string(target_geoscale)

        geoscale_by_fips = pd.concat([
            (geo.filtered_fips(scale)
             .assign(geoscale=scale, National='USA')
             # ^^^ Need to have a column for each relevant scale
             .rename(columns={'FIPS': 'Location'}))
            # ^^^ (only FIPS for now)
            for scale in [s for s in geo.scale if s.has_fips_level]
        ])

        geoscale_name_columns = [s.name.title() for s in geo.scale
                                 if s.has_fips_level]

        log.info('Determining appropriate source geoscale for %s; '
                 'target geoscale is %s',
                 self.full_name,
                 target_geoscale.name.lower())

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

        # if an activity column is a mix of string and np.nan values but
        # after subsetting, the column is all np.nan, then the column dtype is
        # converted to float which causes an error when merging float col back
        # with the original object dtype. So convert float cols back to object
        for df in highest_reporting_level_by_geoscale:
            for c in ['ActivityProducedBy', 'ActivityConsumedBy']:
                if df[c].dtype == float:
                    df[c] = df[c].astype(object)

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
            #   ^^^ max() with axis='columns' takes max along rows
            .query('geoscale == source_geoscale')
            .drop(columns=(['geoscale',
                            *geoscale_name_columns,
                            *reporting_level_columns]))
        )

        if len(fba_at_source_geoscale.source_geoscale.unique()) > 1:
            log.warning('%s has multiple source geoscales: %s',
                        fba_at_source_geoscale.full_name,
                        ', '.join([s.name.lower() for s in
                                   fba_at_source_geoscale
                                   .source_geoscale.unique()]))
        else:
            log.info('%s source geoscale is %s',
                     fba_at_source_geoscale.full_name,
                     fba_at_source_geoscale
                     .source_geoscale.unique()[0].name.lower())

        fba_at_target_geoscale = (
            fba_at_source_geoscale
            .drop(columns='source_geoscale')
            .convert_fips_to_geoscale(target_geoscale)
            .aggregate_flowby()
            .astype({activity: flowby_config['fba_fields'][activity]
                     for activity in ['ActivityProducedBy',
                                      'ActivityConsumedBy']})
        )

        if target_geoscale != geo.scale.NATIONAL:
            # TODO: This block of code can be simplified a great deal once
            #       validation.py is rewritten to use the FB config dictionary
            activities = list(
                self
                .add_primary_secondary_columns('Activity')
                .PrimaryActivity.unique()
            )

            validation.compare_geographic_totals(
                fba_at_target_geoscale, self,
                self.source_name, self.config,
                self.full_name.split('.')[-1], activities,
                df_type='FBS', subnational_geoscale=target_geoscale
                # ^^^ TODO: Rewrite validation to use fb metadata
            )

        return fba_at_target_geoscale

    def map_to_sectors(
            self: 'FlowByActivity',
            target_year: Literal[2002, 2007, 2012, 2017] = 2012,
            external_config_path: str = None
    ) -> 'FlowByActivity':
        """
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
        """
        naics_key = naics.industry_spec_key(self.config['industry_spec'])

        if self.config['sector-like_activities']:
            log.info('Activities in %s are NAICS codes.',
                     self.full_name)

            try:
                source_year = int(self.config
                                  .get('activity_schema', '')[6:10])
            except ValueError:
                source_year = 2012
                log.warning('No NAICS year given for NAICS activities in %s. '
                            '2012 used as default.', self.full_name)
            else:
                log.info('NAICS Activities in %s use NAICS year %s.',
                         self.full_name, source_year)

            if self.config.get('sector_hierarchy') == 'parent-completeChild':
                log.info('NAICS are a mix of parent-completeChild, assigning '
                         'activity columns directly to sector columns')

                # existing naics
                existing_sectors = pd.DataFrame()
                existing_sectors['Sector'] = (
                    pd.Series(self[['ActivityProducedBy',
                                    'ActivityConsumedBy']].values.ravel('F'))
                    .dropna()
                    .drop_duplicates()
                    .reset_index(drop=True)
                    )
                existing_sectors['Activity'] = existing_sectors['Sector']

                # load master crosswalk
                cw = common.load_crosswalk('sector_timeseries')
                sectors = (cw[[f'NAICS_{target_year}_Code']]
                           .drop_duplicates()
                           .dropna()
                           )
                # create list of sectors that exist in original df, which,
                # if created when expanding sector list cannot be added
                naics_df = pd.DataFrame([])
                for i in existing_sectors['Sector']:
                    dig = len(str(i))
                    n = existing_sectors[
                        existing_sectors['Sector'].apply(
                            lambda x: x[0:dig]) == i]
                    if len(n) == 1:
                        expanded_n = sectors[
                            sectors[f'NAICS_{target_year}_Code'].apply(
                                lambda x: x[0:dig] == i)]
                        expanded_n = expanded_n.assign(Sector=i)
                        naics_df = pd.concat([naics_df, expanded_n])

                activity_to_source_naics_crosswalk = (
                    existing_sectors
                    .merge(naics_df, how='left')
                    .assign(Sector=lambda x: np.where(
                        x[f'NAICS_{target_year}_Code'].isna(), x['Sector'],
                        x[f'NAICS_{target_year}_Code']))
                    .drop(columns=[f'NAICS_{target_year}_Code'])
                )

                target_naics = set(
                    naics.industry_spec_key(self.config['industry_spec'])
                    .target_naics)

                activity_to_target_naics_crosswalk = (
                    activity_to_source_naics_crosswalk
                    .query('Sector  in @target_naics')
                )

                fba_w_naics = self
                for direction in ['ProducedBy', 'ConsumedBy']:
                    fba_w_naics = (
                        fba_w_naics
                        .merge(activity_to_target_naics_crosswalk,
                               how='left',
                               left_on=f'Activity{direction}',
                               right_on='Activity')
                        .rename(columns={'Sector': f'Sector{direction}',
                                         'SectorType': f'{direction}SectorType'})
                        .drop(columns=['ActivitySourceName',
                                       'SectorSourceName',
                                       'Activity'],
                              errors='ignore')
                    )
            else:
                # if sector-like activities are aggregated, then map all
                # sectors to target sector level
                log.info('Converting NAICS codes to desired industry/sector '
                         'aggregation structure.')
                fba_w_naics = self.copy()
                # fba_w_naics = self.assign(
                #     ActivitySourceName=self.source_name,
                #     SectorType=np.nan
                # ) ^^ I don't think these fields are necessary in this case
                for direction in ['ProducedBy', 'ConsumedBy']:
                    fba_w_naics = (
                        fba_w_naics
                        .merge(naics_key,
                               how='left',
                               left_on=f'Activity{direction}',
                               right_on='source_naics')
                        .rename(columns={'target_naics': f'Sector{direction}'})
                        .drop(columns='source_naics')
                    )

        else:
            log.info('Getting crosswalk between activities in %s and '
                     'NAICS codes.', self.full_name)
            activity_to_source_naics_crosswalk = (
                sectormapping.get_activitytosector_mapping(
                    # ^^^ TODO: Replace or streamline get_...() function
                    (self.config.get('activity_to_sector_mapping')
                     or self.source_name),
                    fbsconfigpath=external_config_path)
                .astype('object')
                [['Activity', 'Sector', 'SectorType', 'SectorSourceName']]
            )

            source_years = set(
                activity_to_source_naics_crosswalk.SectorSourceName
                .str.removeprefix('NAICS_')
                .str.removesuffix('_Code')
                .dropna().astype('int')
            )
            source_year = (2012 if 2012 in source_years
                           else max(source_years) if source_years
                           else 2012)
            if not source_years:
                log.warning('No NAICS year/sector source name (e.g. '
                            '"NAICS_2012_Code") provided in crosswalk for %s. '
                            '2012 being used as default.',
                            self.full_name)

            activity_to_source_naics_crosswalk = (
                activity_to_source_naics_crosswalk
                .query(f'SectorSourceName == "NAICS_{source_year}_Code"')
                .reset_index(drop=True)
            )

            # only retain the activities in the crosswalk that exist in
            # the FBA. Necessary because the crosswalk could contain parent
            # to child relationships that do not exist in the FBA subset and
            # if those parent-child relationships are kept in the crosswalk,
            # the FBA could be mapped incorrectly
            activities_in_fba = (pd.Series(self[['ActivityProducedBy',
                                                 'ActivityConsumedBy']]
                                           .values.ravel('F'))
                                 .dropna()
                                 .drop_duplicates()
                                 .values.tolist()
                                 )
            activity_to_source_naics_crosswalk = \
                activity_to_source_naics_crosswalk[
                    activity_to_source_naics_crosswalk['Activity'].isin(
                        activities_in_fba)]

            log.info('Converting NAICS codes in crosswalk to desired '
                     'industry/sector aggregation structure.')
            if self.config.get('sector_hierarchy') == 'parent-completeChild':
                existing_sectors = activity_to_source_naics_crosswalk[
                    ['Activity', 'Sector']]

                # create list of sectors that exist in original df, which,
                # if created when expanding sector list cannot be added
                naics_df = pd.DataFrame([])
                for i in existing_sectors['Activity'].unique():
                    existing_sectors_sub = existing_sectors[
                        existing_sectors['Activity'] == i]
                    for j in existing_sectors_sub['Sector']:
                        dig = len(str(j))
                        n = existing_sectors_sub[
                            existing_sectors_sub['Sector'].apply(
                                lambda x: x[0:dig]) == j]
                        if len(n) == 1:
                            expanded_n = naics_key[naics_key['source_naics']
                                                   == j]
                            expanded_n = expanded_n.assign(Activity=i)
                            naics_df = pd.concat([naics_df, expanded_n])

                activity_to_target_naics_crosswalk = (
                    activity_to_source_naics_crosswalk
                    .merge(naics_df,
                           how='left',
                           left_on=['Activity', 'Sector'],
                           right_on=['Activity', 'source_naics'])
                    .assign(Sector=lambda x: x['target_naics'])
                    .drop(columns=['source_naics', 'target_naics'])
                )

                fba_w_naics = self
                for direction in ['ProducedBy', 'ConsumedBy']:
                    fba_w_naics = (
                        fba_w_naics
                        .merge(activity_to_target_naics_crosswalk,
                               how='left',
                               left_on=f'Activity{direction}',
                               right_on='Activity')
                        .rename(columns={'Sector': f'Sector{direction}',
                                         'SectorType': f'{direction}SectorType'})
                        .drop(columns=['ActivitySourceName',
                                       'SectorSourceName',
                                       'Activity'],
                              errors='ignore')
                    )

            else:
                activity_to_target_naics_crosswalk = (
                    activity_to_source_naics_crosswalk
                    .merge(naics_key,
                           how='left',
                           left_on='Sector',
                           right_on='source_naics')
                    .assign(Sector=lambda x: x.target_naics)
                    .drop(columns=['source_naics', 'target_naics'])
                    .drop_duplicates()
                )

                log.info('Mapping activities in %s to NAICS codes using '
                         'crosswalk', self.full_name)
                fba_w_naics = self
                for direction in ['ProducedBy', 'ConsumedBy']:
                    fba_w_naics = (
                        fba_w_naics
                        .merge(activity_to_target_naics_crosswalk,
                               how='left',
                               left_on=f'Activity{direction}',
                               right_on='Activity')
                        .rename(columns={'Sector': f'Sector{direction}',
                                         'SectorType': f'{direction}SectorType'})
                        .drop(columns=['ActivitySourceName',
                                       'SectorSourceName',
                                       'Activity'],
                              errors='ignore')
                    )

            if source_year != target_year:
                log.info('Using NAICS time series/crosswalk to map NAICS '
                         'codes from NAICS year %s to NAICS year %s.',
                         source_year, target_year)
                for direction in ['ProducedBy', 'ConsumedBy']:
                    fba_w_naics = (
                        fba_w_naics
                        .merge(naics.year_crosswalk(source_year, target_year),
                               how='left',
                               left_on=f'Sector{direction}',
                               right_on='source_naics')
                        .assign(**{f'Sector{direction}': lambda x: x.target_naics})
                        .drop(columns=['source_naics', 'target_naics'])
                    )

        # warn if any activities are not mapped to sectors
        not_mapped = fba_w_naics[fba_w_naics[['SectorProducedBy',
                                              'SectorConsumedBy']].isna().all(1)]
        if len(not_mapped) > 0:
            not_mapped = (not_mapped
                          [['ActivityProducedBy', 'ActivityConsumedBy']]
                          .drop_duplicates())
            log.warning('Activities in %s are not mapped to sectors: %s',
                        not_mapped.full_name,
                        set(zip(not_mapped.ActivityProducedBy,
                                not_mapped.ActivityConsumedBy))
                        )
            fba_w_naics = fba_w_naics.dropna(subset=[
                'SectorProducedBy', 'SectorConsumedBy'], how='all')

        return (
            fba_w_naics
            .assign(SectorSourceName=f'NAICS_{target_year}_Code')
            .reset_index(drop=True)
        )

    def equally_attribute(self: 'FlowByActivity') -> 'FlowByActivity':
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
        fba = self.add_primary_secondary_columns('Sector')

        groupby_cols = ['group_id']
        for rank in ['Primary', 'Secondary']:
            fba = (
                fba
                .assign(
                    **{f'_naics_{n}': fba[f'{rank}Sector'].str.slice(stop=n)
                        for n in range(2, 8)},
                    **{f'_unique_naics_{n}_by_group': lambda x, i=n: (
                            x.groupby(groupby_cols if i == 2
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
            groupby_cols.append(f'{rank}Sector')

        return fba.drop(
            columns=['PrimarySector', 'SecondarySector',
                     *[f'_naics_{n}' for n in range(2, 8)],
                     *[f'_unique_naics_{n}_by_group' for n in range(2, 8)]]
        )

    def harmonize_geoscale(
        self: 'FlowByActivity',
        other: 'FlowBySector'
    ) -> 'FlowByActivity':

        fba_geoscale = geo.scale.from_string(self.config['geoscale'])
        other_geoscale = geo.scale.from_string(other.config['geoscale'])

        if other_geoscale < fba_geoscale:
            log.info('Aggregating %s from %s to %s', other.full_name,
                     other_geoscale, fba_geoscale)
            other = (
                other
                .convert_fips_to_geoscale(fba_geoscale)
                .aggregate_flowby()
            )
        elif other_geoscale > fba_geoscale:
            log.info('%s is %s, while %s is %s, so attributing %s to '
                     '%s', other.full_name, other_geoscale, self.full_name,
                     fba_geoscale, other_geoscale, fba_geoscale)
            self = (
                self
                .assign(temp_location=self.Location)
                .convert_fips_to_geoscale(other_geoscale,
                                          column='temp_location')
            )

        fba = self.add_primary_secondary_columns('Sector')

        subset_cols = ['PrimarySector', 'Location', 'FlowAmount', 'Unit']
        groupby_cols = ['PrimarySector', 'Location', 'Unit']
        if self.config.get('attribute_on') is not None:
            subset_cols = ['PrimarySector', 'Location', 'FlowAmount', 'Unit',
                           self.config.get('attribute_on')]
            groupby_cols = ['PrimarySector', 'Location', 'Unit',
                            self.config.get('attribute_on')]
        other = (
            other
            .add_primary_secondary_columns('Sector')
            [subset_cols]
            .groupby(groupby_cols)
            .agg('sum')
            .reset_index()
        )

        return fba_geoscale, other_geoscale, fba, other

    def proportionally_attribute(
        self: 'FlowByActivity',
        other: 'FlowBySector'
    ) -> 'FlowByActivity':
        '''
        This method takes flows from the calling FBA which are mapped to
        multiple sectors and attributes them to those sectors proportionally to
        flows from other (an FBS).
        '''

        log.info('Attributing flows in %s using %s.',
                 self.full_name, other.full_name)

        fba_geoscale, other_geoscale, fba, other = self.harmonize_geoscale(
            other)

        # attribute on sector columns
        if self.config.get('attribute_on') is None:
            groupby_cols = ['group_id']
            for rank in ['Primary', 'Secondary']:
                # skip over Secondary if not relevant
                if fba[f'{rank}Sector'].isna().all():
                    continue
                counted = fba.assign(group_count=(fba.groupby(groupby_cols)
                                                  ['group_id']
                                                  .transform('count')))
                directly_attributed = (
                    counted
                    .query('group_count == 1')
                    .drop(columns='group_count')
                )
                needs_attribution = (
                    counted
                    .query('group_count > 1')
                    .drop(columns='group_count')
                )

                merged = (
                    needs_attribution
                    .merge(other,
                           how='left',
                           left_on=[f'{rank}Sector',
                                    'temp_location'
                                    if 'temp_location' in needs_attribution
                                    else 'Location'],
                           right_on=['PrimarySector', 'Location'],
                           suffixes=[None, '_other'])
                    .fillna({'FlowAmount_other': 0})
                )

                denominator_flag = ~merged.duplicated(subset=[*groupby_cols,
                                                      f'{rank}Sector'])
                with_denominator = (
                    merged
                    .assign(denominator=(
                        merged
                        .assign(FlowAmount_other=(merged.FlowAmount_other
                                                  * denominator_flag))
                        .groupby(groupby_cols)
                        ['FlowAmount_other']
                        .transform('sum')))
                )

                non_zero_denominator = with_denominator.query(f'denominator != 0 ')
                unattributable = with_denominator.query(f'denominator == 0 ')

                if not unattributable.empty:
                    log.warning(
                        'Could not attribute activities in %s due to lack of '
                        'flows in attribution source %s for mapped %s sectors %s',
                        #set(zip(unattributable.ActivityProducedBy,
                        #        unattributable.ActivityConsumedBy,
                        #        unattributable.Location)),
                        unattributable.full_name,
                        other.full_name,
                        rank,
                        sorted(set(unattributable[f'{rank}Sector']))
                    )

                proportionally_attributed = (
                    non_zero_denominator
                    .assign(FlowAmount=lambda x: (x.FlowAmount
                                                  * x.FlowAmount_other
                                                  / x.denominator))
                )

            fba = pd.concat([directly_attributed,
                             proportionally_attributed], ignore_index=True)

        # else attribute on column specified in the FBS yaml
        else:

            log.info(f'Proportionally attributing on {attribute_cols}')
            fba = (fba.add_primary_secondary_columns('Sector'))

            groupby_cols = [self.config.get('attribute_on'), 'Unit']
            attribute_cols = self.config.get('attribute_on')

            other_with_denominator = (
                other
                .assign(denominator=(other
                        .groupby(groupby_cols)['FlowAmount']
                        .transform('sum')))
            )

            with_denominator = (
                fba
                .merge(other_with_denominator,
                       how='left',
                       left_on=[attribute_cols,
                                'temp_location'
                                if 'temp_location' in fba
                                else 'Location'],
                       right_on=[attribute_cols, 'Location'],
                       suffixes=[None, '_other'])
                .fillna({'FlowAmount_other': 0})
            )

            non_zero_denominator = with_denominator.query(f'denominator != 0 ')
            unattributable = with_denominator.query(f'denominator == 0 ')

            if not unattributable.empty:
                log.warning(
                    'Could not attribute activities %s in %s due to lack of '
                    'flows in attribution source %s for mapped %s sectors %s',
                    set(zip(unattributable.ActivityProducedBy,
                            unattributable.ActivityConsumedBy,
                            unattributable.Location)),
                    unattributable.full_name,
                    other.full_name,
                    rank,
                    set(unattributable[rank])
                )

            fba = (
                non_zero_denominator
                .assign(FlowAmount=lambda x: (x.FlowAmount
                                              * x.FlowAmount_other
                                              / x.denominator))
            )

            if self.config.get('fill_sector_column') is not None:
                sector_col = self.config.get('fill_sector_column')
                fba[sector_col] = fba['PrimarySector_other']

        return (
            fba
            .drop(columns=['PrimarySector', 'SecondarySector',
                           'temp_location', 'PrimarySector_other',
                           'Location_other', 'FlowAmount_other', 'denominator',
                           'Unit_other'],
                  errors='ignore')
            .reset_index(drop=True)
        )

    def multiplication_attribution(
        self: 'FlowByActivity',
        other: 'FlowBySector'
    ) -> 'FlowByActivity':
        """
        This method takes flows from the calling FBA which are mapped to
        multiple sectors and multiplies them by flows from other (an FBS).
        """

        log.info('Multiplying flows in %s by %s.',
                 self.full_name, other.full_name)
        fba_geoscale, other_geoscale, fba, other = self.harmonize_geoscale(
            other)

        # todo: update units after multiplying

        # multiply using each dfs primary sector col
        merged = (fba
                  .merge(other,
                         how='left',
                         left_on=['PrimarySector', 'temp_location'
                                  if 'temp_location' in fba
                                  else 'Location'],
                         right_on=['PrimarySector', 'Location'],
                         suffixes=[None, '_other'])
                  .fillna({'FlowAmount_other': 0})
                  )

        fba = (merged
               .assign(FlowAmount=lambda x: (x.FlowAmount
                                             * x.FlowAmount_other))
               .drop(columns=['PrimarySector_other', 'Location_other',
                              'FlowAmount_other', 'denominator'],
                     errors='ignore')
               )

        # determine if any flows are lost because multiplied by 0
        fba_null = fba[fba['FlowAmount'] == 0]
        if len(fba_null) > 0:
            log.warning('FlowAmounts in %s are reset to 0 due to lack of '
                        'flows in attribution source %s for '
                        'ActivityProducedBy/ActivityConsumedBy/Location: %s',
                        fba.full_name, other.full_name,
                        set(zip(fba_null.ActivityProducedBy,
                                fba_null.ActivityConsumedBy,
                                fba_null.Location))
                        )

        return (
            fba
            .drop(columns=['PrimarySector', 'SecondarySector',
                           'temp_location'],
                  errors='ignore')
            .reset_index(drop=True)
        )


    def prepare_fbs(
            self: 'FlowByActivity',
            external_config_path: str = None
            ) -> 'FlowBySector':
        if 'activity_sets' in self.config:
            try:
                return (
                    pd.concat([
                        fba.prepare_fbs(external_config_path=external_config_path)
                        for fba in (
                            self
                            .select_by_fields()
                            .function_socket('clean_fba_before_activity_sets')
                            .activity_sets()
                        )
                    ])
                    .reset_index(drop=True)
                )
            except ValueError:
                return FlowBySector(pd.DataFrame())

        # Primary FlowBySector generation approach:
        return FlowBySector(
            self
            .function_socket('clean_fba_before_mapping')
            .select_by_fields()
            .function_socket('estimate_suppressed')
            .select_by_fields(selection_fields=self.config.get(
                'selection_fields_after_data_suppression_estimation'))
            .convert_units_and_flows()  # and also map to flow lists
            .function_socket('clean_fba')
            .convert_to_geoscale()
            .attribute_flows_to_sectors(external_config_path=external_config_path)  # recursive call to prepare_fbs
            .function_socket('clean_fba_after_attribution')
            .drop(columns=['ActivityProducedBy', 'ActivityConsumedBy'])
            .aggregate_flowby()
        )

    def activity_sets(self) -> List['FlowByActivity']:
        '''
        This function breaks up an FBA dataset into its activity sets, if its
        config dictionary specifies activity sets, and returns a list of the
        resulting FBAs. Otherwise, it returns a list containing the calling
        FBA.

        Activity sets are determined by the selection_field key under each
        activity set name. An error will be logged if any rows from the calling
        FBA are assigned to multiple activity sets.
        '''
        if 'activity_sets' not in self.config:
            return [self]

        log.info('Splitting %s into activity sets', self.full_name)
        activities = self.config['activity_sets']
        parent_config = {k: v for k, v in self.config.items()
                         if k not in ['activity_sets',
                                      'clean_fba_before_activity_sets']
                         and not k.startswith('_')}
        parent_fba = self.reset_index().rename(columns={'index': 'row'})

        child_fba_list = []
        assigned_rows = set()
        for activity_set, activity_config in activities.items():
            log.info('Creating FlowByActivity for %s', activity_set)

            child_fba = (
                parent_fba
                .add_full_name(
                    f'{parent_fba.full_name}{NAME_SEP_CHAR}{activity_set}')
                .select_by_fields(
                    selection_fields=activity_config.get('selection_fields'),
                    exclusion_fields=activity_config.get('exclusion_fields'))
            )

            child_fba.config = {**parent_config, **activity_config}
            child_fba = child_fba.assign(SourceName=child_fba.full_name)

            if set(child_fba.row) & assigned_rows:
                log.critical(
                    'Some rows from %s assigned to multiple activity '
                    'sets. This will lead to double-counting:\n%s',
                    parent_fba.full_name,
                    child_fba.query(
                        f'row in {list(set(child_fba.row) & assigned_rows)}'
                    )
                )
                # raise ValueError('Some rows in multiple activity sets')

            assigned_rows.update(child_fba.row)
            if not child_fba.empty:
                child_fba_list.append(child_fba.drop(columns='row'))
            else:
                log.error('Activity set %s is empty. Check activity set '
                          'definition!', child_fba.full_name)

        if set(parent_fba.row) - assigned_rows:
            log.warning('Some rows from %s not assigned to an activity '
                        'set. Is this intentional?', parent_fba.full_name)
            unassigned = parent_fba.query('row not in @assigned_rows')

        return child_fba_list

    def convert_units_and_flows(
        self: 'FlowByActivity'
    ) -> 'FlowByActivity':
        if 'emissions_factors' in self.config:
            self = self.convert_activity_to_emissions()
        if 'adjustment_factor' in self.config:
            # ^^^ TODO: There has to be a better way to do this.
            self = self.assign(FlowAmount=self.FlowAmount
                               * self.config['adjustment_factor'])

        self = self.convert_daily_to_annual()
        if self.config.get('fedefl_mapping'):
            mapped = self.map_to_fedefl_list(
                drop_unmapped_rows=self.config.get('drop_unmapped_rows', False)
                )
        else:
            mapped = self.rename(columns={'FlowName': 'Flowable',
                                          'Compartment': 'Context'})
        return (mapped.standardize_units())

    def convert_activity_to_emissions(
        self: 'FlowByActivity'
    ) -> 'FlowByActivity':
        '''
        This method converts flows of an activity (most commonly a measure of
        fuel burned) into flows of one or more pollutants. This is a first
        draft, so it may need some refinement.

        Emissions factors may be specified in a .csv file, with whatever
        columns need to be matched on for accurate conversion from activity to
        emissions.
        '''
        emissions_factors = (
            pd.read_csv(
                f'{settings.datapath}{self.config["emissions_factors"]}.csv')
            .drop(columns='source')
        )

        emissions_fba = (
            self
            .merge(emissions_factors, how='left')
            .assign(FlowName=lambda x: x.pollutant,
                    FlowAmount=lambda x: x.FlowAmount * x.emissions_factor,
                    Unit=lambda x: x.target_unit,
                    Class='Chemicals',
                    FlowType='ELEMENTARY_FLOW')
            .drop(columns=['pollutant', 'target_unit', 'emissions_factor'])
            .add_primary_secondary_columns('Activity')
            .assign(ActivityProducedBy=lambda x: x.PrimaryActivity,
                    ActivityConsumedBy=lambda x: x.SecondaryActivity)
            # ^^^ TODO: This is a line I'm quite skeptical of. There's got to
            #     be a better way to do this. Maybe specify in the config?
            .drop(columns=['PrimaryActivity', 'SecondaryActivity'])
        )
        return emissions_fba


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
        config: dict = None,
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
            FBS from EPA's remote server rather than generating it
            (if not found locally)
        :kwargs: keyword arguments to pass to _getFlowBy(). Possible kwargs
            include full_name and config.
        :return: FlowBySector dataframe
        '''
        file_metadata = metadata.set_fb_meta(method, 'FlowBySector')

        if config is None:
            try:
                config = common.load_yaml_dict(method, 'FBS',
                                               external_config_path)
            except exceptions.FlowsaMethodNotFoundError:
                config = {}

        flowby_generator = (
            lambda x=method, y=external_config_path, z=download_sources_ok:
                cls.generateFlowBySector(x, y, z)
            )
        return super()._getFlowBy(
            file_metadata=file_metadata,
            download_ok=download_fbs_ok,
            flowby_generator=flowby_generator,
            output_path=settings.fbsoutputpath,
            full_name=method,
            config=config,
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

        # Cache one or more sources by attaching to method_config
        to_cache = method_config.pop('sources_to_cache', {})
        if 'cache' in method_config:
            log.warning('Config key "cache" for %s about to be overwritten',
                        method)

        method_config['cache'] = {}
        for source_name, config in to_cache.items():
            method_config['cache'][source_name] = (
                get_flowby_from_config(
                    name=source_name,
                    config={
                        **method_config,
                        'method_config_keys': method_config.keys(),
                        **get_catalog_info(source_name),
                        **config
                    },
                    external_config_path=external_config_path,
                    download_sources_ok=download_sources_ok
                ).prepare_fbs(external_config_path=external_config_path)
            )
            # ^^^ This is done with a for loop instead of a dict comprehension
            #     so that later entries in method_config['sources_to_cache']
            #     can make use of the cached copy of an earlier entry.

        # Generate FBS from method_config
        sources = method_config.pop('source_names')

        fbs = pd.concat([
            get_flowby_from_config(
                name=source_name,
                config={
                    **method_config,
                    'method_config_keys': method_config.keys(),
                    **get_catalog_info(source_name),
                    **config
                },
                external_config_path=external_config_path,
                download_sources_ok=download_sources_ok
            ).prepare_fbs(external_config_path=external_config_path)
            for source_name, config in sources.items()
        ])

        fbs.full_name = method
        fbs.config = method_config

        # drop year from LocationSystem for FBS use with USEEIO
        fbs['LocationSystem'] = fbs['LocationSystem'].str.split('_').str[0]
        # aggregate to target geoscale
        fbs = (
            fbs
            .convert_fips_to_geoscale(
                geo.scale.from_string(fbs.config.get('geoscale')))
            .aggregate_flowby()
        )
        # aggregate to target sector
        fbs = fbs.sector_aggregation()

        # set all data quality fields to none until implemented fully
        log.info('Reset all data quality fields to None')
        dq_cols = ['Spread', 'Min', 'Max',
                   'DataReliability', 'TemporalCorrelation',
                   'GeographicalCorrelation', 'TechnologicalCorrelation',
                   'DataCollection']
        fbs = fbs.assign(**dict.fromkeys(dq_cols, None))

        # Save fbs and metadata
        log.info(f'FBS generation complete, saving {method} to file')
        meta = metadata.set_fb_meta(method, 'FlowBySector')
        esupy.processed_data_mgmt.write_df_to_file(fbs, settings.paths, meta)
        reset_log_file(method, meta)
        metadata.write_metadata(source_name=method,
                                config=common.load_yaml_dict(
                                    method, 'FBS', external_config_path),
                                fb_meta=meta,
                                category='FlowBySector')

        return fbs

    def sector_aggregation(self):
        """
        In the event activity sets in an FBS are at a less aggregated target
        sector level than the overall target level, aggregate the sectors to
        the FBS target scale
        :return:
        """
        naics_key = naics.industry_spec_key(self.config['industry_spec'])

        fbs = self
        for direction in ['ProducedBy', 'ConsumedBy']:
            fbs = (
                fbs
                .rename(columns={f'Sector{direction}': 'source_naics'})
                .merge(naics_key,
                       how='left')
                .rename(columns={'target_naics': f'Sector{direction}'})
                .drop(columns='source_naics')
                .aggregate_flowby()
            )

        return fbs

    def prepare_fbs(
        self: 'FlowBySector',
        external_config_path: str = None
    ) -> 'FlowBySector':
        if 'activity_sets' in self.config:
            try:
                return (
                    pd.concat([
                        fbs.prepare_fbs()
                        for fbs in (
                            self
                            .select_by_fields()
                            .activity_sets()
                        )
                    ])
                    .reset_index(drop=True)
                )
            except ValueError:
                return FlowBySector(pd.DataFrame())
        return (
            self
            .function_socket('clean_fbs')
            .select_by_fields()
            .sector_aggregation()  # convert to proper industry spec.
            .convert_fips_to_geoscale()
            .attribute_flows_to_sectors()
            .aggregate_flowby()  # necessary after consolidating geoscale
        )


    def display_tables(
        self: 'FlowBySector',
        display_tables: dict = None
    ) -> pd.DataFrame:
        display_tables = display_tables or self.config.get('display_tables')
        if display_tables is None:
            log.error('Cannot generate display tables, since no configuration '
                      'is specified for them')
            return None

        def convert_industry_spec(
            fb_at_source_naics: 'FlowBySector',
            industry_spec: dict = None
        ) -> 'FlowBySector':
            '''
            This is here because it's only for display purposes. It can be
            replaced once there's a proper method for converting an FBS to
            a new industry_spec
            '''
            if industry_spec is None:
                return fb_at_source_naics
            fb_at_target_naics = (
                fb_at_source_naics
                .merge(naics.industry_spec_key(industry_spec),
                       how='left',
                       left_on='SectorProducedBy', right_on='source_naics')
                .assign(
                    SectorProducedBy=lambda x:
                        x.SectorProducedBy.mask(x.SectorProducedBy.str.len()
                                                >= x.target_naics.str.len(),
                                                x.target_naics)
                )
                .drop(columns=['target_naics', 'source_naics'])
                .aggregate_flowby()
            )
            return fb_at_target_naics

        table_dict = {
            table_name: (
                self
                .select_by_fields(table_config.get('selection_fields'))
                .pipe(convert_industry_spec, table_config.get('industry_spec'))
                [['Flowable', 'Unit', 'SectorProducedBy', 'FlowAmount']]
                .rename(columns={'Flowable': 'Pollutant',
                                 'SectorProducedBy': 'Industry',
                                 'FlowAmount': 'Amount'})
                .replace(table_config.get('replace_dict', {}))
                .assign(Pollutant=lambda x: x.Pollutant + ' (' + x.Unit + ')')
                .drop(columns='Unit')
                .groupby(['Pollutant', 'Industry']).agg('sum')
                .reset_index()
                .pivot(index='Pollutant', columns='Industry', values='Amount')
            )
            for table_name, table_config in display_tables.items()
        }

        tables_path = (f'{settings.tableoutputpath}{self.full_name}'
                       f'_Display_Tables.xlsx')
        try:
            with ExcelWriter(tables_path) as writer:
                for name, table in table_dict.items():
                    table.to_excel(writer, name)
        except PermissionError:
            log.warning(f'Permission to write display tables for '
                        f'{self.full_name} to {tables_path} denied.')

        return table_dict


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
