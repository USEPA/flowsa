"""
The functions defined in this script can be applied to both
FlowByActivity and FlowBySector classes.
"""

from typing import List, Literal, TypeVar, TYPE_CHECKING
import pandas as pd
import numpy as np
import re
from functools import partial, reduce
from copy import deepcopy
from flowsa import (settings, literature_values, flowsa_yaml, geo, schema,
                    naics)
from flowsa.common import get_catalog_info
from flowsa.flowsa_log import log, vlog
from flowsa.location import fips_number_key
import esupy.processed_data_mgmt
import esupy.dqi

if TYPE_CHECKING:
    from flowsa.flowbysector import FlowBySector
    from flowsa.flowbyactivity import FlowByActivity

FB = TypeVar('FB', bound='_FlowBy')
S = TypeVar('S', bound='_FlowBySeries')
NAME_SEP_CHAR = '.'
# ^^^ Used to separate source/activity set names as part of 'full_name' attr


with open(settings.datapath / 'flowby_config.yaml') as f:
    flowby_config = flowsa_yaml.load(f)
    # ^^^ Replaces schema.py


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
    from flowsa.flowbyactivity import FlowByActivity
    from flowsa.flowbysector import FlowBySector

    external_data_path = config.get('external_data_path')

    if config.get('data_format') == 'FBA':
        return FlowByActivity.return_FBA(
            full_name=name,
            config=config,
            download_ok=download_sources_ok,
            external_data_path=external_data_path
        )
    elif config.get('data_format') == 'FBS':
        return FlowBySector.return_FBS(
            method=name,
            config=config,
            external_config_path=external_config_path,
            download_sources_ok=download_sources_ok,
            download_fbs_ok=download_sources_ok,
            external_data_path=external_data_path
        )
    elif config.get('data_format') == 'FBS_outside_flowsa':
        return FlowBySector(  # todo: add convert_df_to_flowby=True?
            config['FBS_datapull_fxn'](
                config=config,
                external_config_path=external_config_path,
                full_name=name
            ),
            full_name=name,
            config=config
        )
    else:
        log.critical(f'Unrecognized `config["data_format"] = '
                     f'{config.get("data_format")}` for source {name}')
        raise ValueError('Unrecognized data format, check assignment in '
                         '.../flowsa/methods/method_status.yaml or assign '
                         'within the method yaml. Data formats allowed: '
                         '"FBA", "FBS", "FBS_outside_flowsa".')




class _FlowBy(pd.DataFrame):
    _metadata = ['full_name', 'config']

    full_name: str
    config: dict

    def __init__(
        self,
        data: pd.DataFrame or '_FlowBy' = None,
        *args,
        # needs to initially be false bc otherwise all internal pandas fxns (.assign(), .copy()) trigger the code to run
        convert_df_to_flowby: bool = False,
        add_missing_columns: bool = True,
        fields: dict = None,
        column_order: List[str] = None,
        string_null: 'np.nan' or None = np.nan,
        **kwargs
    ) -> None:
        '''
        Extends pandas DataFrame. Attaches metadata if provided as kwargs and
        ensures that all columns described in flowby_config.yaml are present
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
        # only runs if truly a pandas df and not retriggered due to .copy() or .assign()
        if isinstance(data, pd.DataFrame) and fields is not None:
            if convert_df_to_flowby and add_missing_columns:
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

            # avoid warning: "Downcasting object dtype arrays on .fillna, .ffill, .bfill is deprecated
            # and will change in a future version"
            with pd.option_context('future.no_silent_downcasting', True):
                data = (data
                    .fillna(fill_na_dict).infer_objects(copy=False)
                    .replace(null_string_dict).infer_objects(copy=False)
                    .astype(fields)
                    )

        if isinstance(data, pd.DataFrame) and column_order is not None and convert_df_to_flowby:
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
                if self[x].dtype in ['int', 'object', 'int32', 'int64']
                and x not in ['Description', 'group_id']]

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
            log.info(f'Attempting to {attempt} {file_metadata.name_data} '
                     f'{file_metadata.category}')
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
                log.info(f'{file_metadata.name_data} {file_metadata.category} '
                         f'not found in {paths.local_path}')
            else:
                log.info(f'Successfully loaded {file_metadata.name_data} '
                         f'{file_metadata.category} from {output_path}')
                break
        else:
            log.error(f'{file_metadata.name_data} {file_metadata.category} '
                      f'could not be found locally, downloaded, or generated')
        fb = cls(df, full_name=full_name or '', config=config or {}, convert_df_to_flowby=True)
        return fb

    def convert_daily_to_annual(self: FB) -> FB:
        daily_list = ['/d', '/day']
        if any(self.Unit.str.endswith(tuple(daily_list))):
            log.info(f'Converting daily flows '
                     f'{[unit for unit in self.Unit.unique() if any(x in unit for x in daily_list)]} '
                     f'to annual')
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
            pd.read_csv(settings.datapath / 'unit_conversion.csv'),
            pd.DataFrame([{
                        'old_unit': 'Canadian Dollar',
                        'new_unit': 'USD',
                        'conversion_factor': 1 / exchange_rate
            }])
        ])
        # avoid warning: "Downcasting object dtype arrays on .fillna, .ffill, .bfill is deprecated
        # and will change in a future version"
        with pd.option_context('future.no_silent_downcasting', True):
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
        exclusion_fields: dict = None,
        skip_select_by: bool = False,
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
        if skip_select_by:
            return self
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

        if selection_fields is None or selection_fields == 'null':
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

        filtered_fb = self.copy()
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
            log.warning(f'{filtered_fb.full_name} FBA is empty')

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
            .infer_objects(copy=False)
            .drop(columns=['PrimaryActivity', 'PrimarySector'],
                  errors='ignore')
            .reset_index(drop=True)
        )
        # Reset blank values to nan
        for k in replace_dict.keys():
            if all(replaced_fb[k] == ''):
                replaced_fb[k] = np.nan

        return replaced_fb

    def aggregate_flowby(
            self: FB,
            columns_to_group_by: List[str] = None,
            columns_to_average: List[str] = None,
            retain_zeros: bool = False,
            aggregate_ratios: bool =False
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
        # if units are rates or ratios, do not aggregate
        if (self['Unit'].str.contains('/').any()) and (aggregate_ratios
                                                       is False):
            log.info(f"At least one row is a rate or ratio with units "
                     f"{self['Unit'].unique().tolist()}, returning df "
                     f"without aggregating")
            return self

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
        if len(self) == 0:
            log.warning('Error, dataframe is empty')
            return self
        aggregated = (
            fb
            .assign(**{f'_{c}_weighted': fb[c] * fb.FlowAmount
                    for c in columns_to_average},
                    **{f'_{c}_weights': fb.FlowAmount * fb[c].notnull()
                    for c in columns_to_average})
            .groupby(columns_to_group_by, dropna=False)
            .agg("sum")
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

        # reset the group total after aggregating
        if 'group_total' in self.columns:
            aggregated = aggregated.assign(group_total=aggregated[
                'FlowAmount'])

        # check flowamounts equal after aggregating
        self_flow = self['FlowAmount'].sum()
        agg_flow = aggregated['FlowAmount'].sum()
        percent_inc = int(((agg_flow - self_flow) * 100) / self_flow)
        if percent_inc > 0:
            log.warning(f'There is an error in aggregating dataframe, as new '
                        'flow totals do not match original dataframe '
                        'flowtotals, there is a {percent_inc}% difference.')

        return aggregated

    def attribute_flows_to_sectors(
        self: FB,
        external_config_path: str = None,
        download_sources_ok: bool = True
    ) -> FB:
        """
        The calling FBA has its activities mapped to sectors, then its flows
        attributed to those sectors, by the methods specified in the calling
        FBA's configuration dictionary.
        """

        from flowsa.flowbyactivity import FlowByActivity

        # look for the "attribute" key in the FBS yaml, which will exist if
        # there are multiple, non-recursive attribution methods applied to a
        # data source
        if "attribute" in self.config:
            attribute_config = self.config.get('attribute')
        # if there is only a single attribution source (or if attribution is
        # fully recursive), then load the config file as is
        else:
            attribute_config = self.config.copy()

        if isinstance(attribute_config, dict):
            attribute_config = [attribute_config]

        for index, step_config in enumerate(attribute_config):
            validate = True
            grouped: 'FB' = (
                self
                .reset_index(drop=True)
                .reset_index(names='group_id')
                .assign(group_total=lambda x: x.FlowAmount)
            )
            if len(grouped) == 0:
                log.warning(f'No data remaining in {self.full_name}.')
                return self
            if index > 0:
                # On subsequent attributions, do not re-clean or append
                # additional sector columns
                fb = grouped.copy()
            elif self.config['data_format'] in ['FBA', 'FBS_outside_flowsa']:
                fb: 'FlowByActivity' = (
                    grouped
                    .map_to_sectors(
                        target_year=self.config['target_naics_year'],
                        external_config_path=external_config_path)
                    .function_socket('clean_fba_w_sec',
                                     attr=self.config,
                                     method=self.config)
                    .rename(columns={'SourceName': 'MetaSources'})
                )
            elif self.config['data_format'] in ['FBS']:
                # ensure sector year of loaded FBS matches target sector year
                if f"NAICS_{self.config['target_naics_year']}_Code" != \
                        grouped['SectorSourceName'][0]:
                    grouped = naics.convert_naics_year(
                        grouped,
                        f"NAICS_{self.config['target_naics_year']}_Code",
                        grouped['SectorSourceName'][0],
                        dfname=self.full_name)
                # convert to proper industry spec.
                fb = grouped.sector_aggregation()

            # subset the fb configuration so it only includes the
            # attribution_method currently being assessed - rather than all
            # consecutive attribution methods/info
            parent_config = {k: v for k, v in self.config.items()
                             if k not in ['activity_sets',
                                          'clean_fba_before_activity_sets']
                             and not k.startswith('_')}

            fb.config = {**parent_config, **step_config}

            attribution_method = step_config.get('attribution_method', 'direct')
            if 'attribution_source' in step_config:
                if isinstance(step_config['attribution_source'], str):
                     attribution_name = step_config['attribution_source']
                else:
                    for k, v in step_config['attribution_source'].items():
                        attribution_name = k

            if attribution_method in ['direct', 'inheritance', 'equal']:
                fb = fb.assign(AttributionSources='Direct')
            else:
                fb = fb.assign(AttributionSources=attribution_name)

            if attribution_method == 'proportional':
                log.info(f"Proportionally attributing {self.full_name} to "
                         f"target sectors with {attribution_name}")
                attribution_fbs = fb.load_prepare_attribution_source(
                    attribution_config=step_config,
                    download_sources_ok=download_sources_ok
                )
                attributed_fb = fb.proportionally_attribute(attribution_fbs)

            elif attribution_method == 'multiplication':
                log.info(f"Multiplying {self.full_name} by {attribution_name}")
                attribution_fbs = fb.load_prepare_attribution_source(
                    attribution_config=step_config,
                    download_sources_ok=download_sources_ok
                )
                attributed_fb = fb.multiplication_attribution(attribution_fbs)

            elif attribution_method == 'division':
                log.info(f"Dividing {self.full_name} by {attribution_name}")
                attribution_fbs = fb.load_prepare_attribution_source(
                    attribution_config=step_config,
                    download_sources_ok=download_sources_ok
                )
                attributed_fb = fb.division_attribution(attribution_fbs)

            elif attribution_method == 'inheritance':
                log.info(f'Directly attributing {self.full_name} to sectors, '
                         f'child sectors inherit parent values.')
                attributed_fb = fb.copy()

            elif attribution_method == 'equal':
                log.info(f"Equally attributing {self.full_name} to "
                         f"target sectors.")
                attributed_fb = fb.equally_attribute()

            elif attribution_method != 'direct':
                log.error(f'Attribution method for {fb.full_name} not '
                          f'recognized: {attribution_method}')
                raise ValueError('Attribution method not recognized')

            else:
                if all(fb.groupby('group_id')['group_id'].agg('count') == 1):
                    log.info(f'No allocation needed for {fb.full_name} at '
                             f'the given industry aggregation level.')
                    attributed_fb = fb.copy()
                    validate = False
                else:
                    # issue warning if the attribution method is missing
                    # from the method yaml
                    if step_config.get('attribution_method') is None:
                        log.warning(f'No allocation method specified for '
                                    f'{fb.full_name}. Using equal allocation '
                                    f'as default.')
                    # issue warning if "direct" attribution is specified,
                    # but the method requires an "equal" allocation
                    elif not all(fb.groupby('group_id')['group_id'].agg(
                            'count') == 1):
                        log.warning(f'Allocation of {fb.full_name} includes '
                                    f'1:many mappings of activities:sectors. '
                                    f'Using "equal" attribution instead of '
                                    f'"direct".')
                    log.info(f"Equally allocating {self.full_name} to "
                             f"target sectors.")
                    attributed_fb = fb.equally_attribute()

            # depending on att method, check that new df values equal
            # original df values
            if validate and attribution_method not in [
                    'multiplication', 'inheritance', 'division']:
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
                                'SectorProducedBy', 'SectorConsumedBy', 'Location',
                                'FlowAmount', 'group_total', 'validation_total']])
                    log.error(f'Errors in attributing flows from '
                              f'{self.full_name}:\n{errors}')
                # calculate the percent change in df caused by attribution
                fbsum = (fb[['group_id', 'group_total']]
                         .drop_duplicates())['group_total'].sum()
                attsum = (validation_fb[['group_id', 'validation_total']]
                          .drop_duplicates())['validation_total'].sum()
                percent_change = round(((attsum - fbsum)/fbsum)*100,3)
                if percent_change == 0:
                    log.info(f"No change in {self.full_name} FlowAmount after "
                             "attribution.")
                else:
                    log.warning(f"Percent change in {self.full_name} after "
                                f"attribution is {percent_change}%")

            # run function to clean fbs after attribution
            attributed_fb = attributed_fb.function_socket(
                'clean_fba_after_attribution')

            # Drop columns created for disaggregation, plus any
            # specified in the config file
            self = (
                attributed_fb
                .drop(columns=['group_id', 'group_total', 'group_count',
                               'FlowAmount_ds', 'factor', 'Suppressed',
                               'descendants'], errors='ignore')
                .drop(columns=step_config.get('drop_columns', []))
            )

        return self

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

        log.info(f'Splitting {self.full_name} into activity sets')
        activities = self.config['activity_sets']
        parent_config = {k: v for k, v in self.config.items()
                         if k not in ['activity_sets',
                                      'clean_fba_before_activity_sets']
                         and not k.startswith('_')}
        parent_df = (self
                     .reset_index(names='row'))

        child_df_list = []
        assigned_rows = set()
        for activity_set, activity_config in activities.items():
            log.info(f'Creating subset for {activity_set}')

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
                log.critical(f"Some rows from {parent_df.full_name} assigned "
                             f"to multiple activity sets. This will lead to "
                             f"double-counting:"
                             f"\n{child_df.query(f'row in {list(set(child_df.row) & assigned_rows)}')}")
                # raise ValueError('Some rows in multiple activity sets')

            assigned_rows.update(child_df.row)
            if not child_df.empty:
                child_df_list.append(child_df.drop(columns='row'))
            else:
                log.error(f'Activity set {child_df.full_name} is empty. '
                          'Check activity set definition!')

        if set(parent_df.row) - assigned_rows:
            log.warning(f'Some rows from {parent_df.full_name} not assigned '
                        f'to an activity set. Is this intentional?')
            unassigned = parent_df.query('row not in @assigned_rows')

        return child_df_list

    def load_prepare_attribution_source(
        self: FB,
        attribution_config=None,
        download_sources_ok: bool = True
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
                **{k: v for k, v in self.config.items()
                           if k in self.config['method_config_keys']},
                **get_catalog_info(name),
                **config,
                'data_format': 'FBS'
            }
            attribution_fbs = attribution_fbs.prepare_fbs(
                download_sources_ok=download_sources_ok)
        else:
            attribution_fbs = get_flowby_from_config(
                name=name,
                config={**{k: v for k, v in self.config.items()
                           if k in self.config['method_config_keys']
                           or k == 'method_config_keys'},
                        **get_catalog_info(name),
                        **config},
                download_sources_ok=download_sources_ok
            ).prepare_fbs(download_sources_ok=download_sources_ok)

        return attribution_fbs

    def harmonize_geoscale(
        self: 'FB',
        other: 'FlowBySector'
    ) -> 'FlowByActivity':

        fb_geoscale = geo.scale.from_string(self.config['geoscale'])
        other_geoscale = geo.scale.from_string(other.config['geoscale'])

        log.info(f"Harmonizing {self.full_name} {self.config['geoscale']} data "
                 f"with {other.full_name} {other.config['geoscale']} data")

        fill_cols = self.config.get('fill_columns')
        if fill_cols and 'Location' in fill_cols:
            # Don't harmonize geoscales when updating Location
            pass
        elif other_geoscale < fb_geoscale:
            log.info(f'Aggregating {other.full_name} from {other_geoscale} to '
                     f'{fb_geoscale}')
            other = (
                other
                .convert_fips_to_geoscale(fb_geoscale)
                .aggregate_flowby()
            )
        elif other_geoscale > fb_geoscale:
            log.info(f'{other.full_name} is {other_geoscale}, while '
                     f'{self.full_name} is {fb_geoscale}, so attributing '
                     f'{other_geoscale} to {fb_geoscale}')
            self = (
                self
                .assign(temp_location=self.Location)
                .convert_fips_to_geoscale(other_geoscale,
                                          column='temp_location')
            )

        fb = self.add_primary_secondary_columns('Sector')

        subset_cols = ['PrimarySector', 'Location', 'FlowAmount', 'Unit']
        groupby_cols = ['PrimarySector', 'Location', 'Unit']
        attribution_cols = self.config.get('attribute_on')
        if attribution_cols is not None:
            subset_cols = subset_cols + attribution_cols
            groupby_cols = subset_cols + attribution_cols
        if fill_cols is not None:
            subset_cols = subset_cols + [fill_cols]
            groupby_cols = groupby_cols + [fill_cols]
        # ensure no duplicates
        subset_cols = list(set(subset_cols))
        groupby_cols = list(set(groupby_cols))

        other = (
            other
            .add_primary_secondary_columns('Sector')
            [subset_cols]
            .groupby(groupby_cols)
            .agg('sum')
            .reset_index()
        )

        return fb_geoscale, other_geoscale, fb, other

    def proportionally_attribute(
        self: 'FB',  # flowbyactivity or flowbysector
        other: 'FlowBySector'
    ) -> 'FlowByActivity':
        """
        This method takes flows from the calling FBA which are mapped to
        multiple sectors and attributes them to those sectors proportionally to
        flows from other (an FBS).
        """

        log.info(f'Attributing flows in {self.full_name} using '
                 f'{other.full_name}.')

        fb_geoscale, other_geoscale, fb, other = self.harmonize_geoscale(other)

        # attribute on sector columns
        if self.config.get('attribute_on') is None:
            groupby_cols = ['group_id']
            for rank in ['Primary', 'Secondary']:
                # skip over Secondary if not relevant
                if fb[f'{rank}Sector'].isna().all():
                    continue
                counted = fb.assign(group_count=(fb.groupby(groupby_cols)
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
                    # implode the location data to shorten warning message
                    unatt_sub = unattributable.groupby(
                        [f"{rank}Sector"], dropna=False, as_index=False).agg(
                        {'Location': lambda x: ", ".join(x)})
                    vlog.warning(
                        f'Could not attribute activities in '
                        f'{unattributable.full_name} due to lack of flows in '
                        f'attribution source {other.full_name} for mapped '
                        f'{rank} sectors/Location '
                        f'{sorted(set(zip(unatt_sub[f"{rank}Sector"], unatt_sub.Location)))}. '
                        f'See validation_log for details.')
                    if other_geoscale.aggregation_level < 5:
                        vlog.warning('This can occur when combining datasets '
                                     'at a sub-national level when activities '
                                     'do not align for some locations.')
                        vlog.warning(f'{other.full_name} is at geoscale '
                                     f'{other_geoscale}. Is that correct?')
                    vlog.debug(
                        'Unattributed activities: \n {}'.format(
                            unattributable
                            .drop(columns=schema.dq_fields +
                                  ['LocationSystem', 'SectorSourceName',
                                   'FlowType', 'ProducedBySectorType',
                                   'ConsumedBySectorType', 'denominator',
                                   'Suppressed'], errors='ignore'
                                  ).to_string()))

                proportionally_attributed = (
                    non_zero_denominator
                    .assign(FlowAmount=lambda x: (x.FlowAmount
                                                  * x.FlowAmount_other
                                                  / x.denominator))
                )

            fb = pd.concat([directly_attributed,
                             proportionally_attributed], ignore_index=True)

        # else attribute on column specified in the FBS yaml
        else:
            attribute_cols = self.config.get('attribute_on')

            log.info(f'Proportionally attributing on {attribute_cols}')
            left_on = attribute_cols + ['temp_location' if 'temp_location'
                                        in fb else 'Location']
            right_on = attribute_cols + ['Location']
            # if replacing df location with "other" location, drop location from merge columns
            if 'Location' in self.config.get('fill_columns', []):
                for l in (left_on, right_on):
                    l.remove('Location')
                # if merging state with county data, merge on first 2 digits of location column using
                # temporary "temp_location" col
                if (self.config['geoscale'] == 'state') & (other.config['geoscale'] == 'county'):
                    fb['temp_location'] = fb['Location'].str[:2]
                    other['temp_location'] = other['Location'].str[:2]
                    for l in (left_on, right_on):
                        l.append('temp_location')

            merged = (
                fb
                .merge(other,
                       how='left',
                       left_on=left_on,
                       right_on=right_on,
                       suffixes=[None, '_other'])
                .fillna({'FlowAmount_other': 0})
            )

            merged_with_denominator = (
                merged
                .assign(denominator=(merged
                        .groupby('group_id')['FlowAmount_other']
                        .transform('sum')))
            )

            non_zero_denominator = merged_with_denominator.query(f'denominator != 0 ')
            unattributable = merged_with_denominator.query(f'denominator == 0 ')

            if not unattributable.empty:
                vlog.warning(
                    'Could not attribute activities in %s due to lack of '
                    'flows in attribution source %s for %s. '
                    'See validation_log for details.',
                    unattributable.full_name,
                    other.full_name,
                    sorted(set(zip(unattributable.SectorProducedBy.fillna('N/A'),
                                   unattributable.SectorConsumedBy.fillna('N/A'),
                                   unattributable.Location)))
                )
                vlog.debug(
                    'Unattributed activities: \n {}'.format(
                        unattributable
                        .drop(columns=schema.dq_fields +
                              ['LocationSystem', 'SectorSourceName', 'FlowType',
                               'ProducedBySectorType', 'ConsumedBySectorType',
                               'denominator', 'Suppressed'],
                              errors='ignore')
                        .to_string()))
            fb = (
                non_zero_denominator
                .assign(FlowAmount=lambda x: (x.FlowAmount
                                              * x.FlowAmount_other
                                              / x.denominator))
            )
        # if the fbs method yamls specifies that a column from in the
        # primary data source should be replaced with data from the
        # attribution source, fill here
        fill_col = self.config.get('fill_columns')
        if fill_col is not None:
            log.info(f'Replacing {fill_col} values in primary data source '
                     f'with those from attribution source.')
            fb[fill_col] = fb[f'{fill_col}_other']

        # drop rows where 'FlowAmount_other' is 0 because the primary
        # activities are not attributed to those sectors. The values are 0
        # because the primary activities are initially assigned to all
        # possible sectors and there is no data for those sectors in the
        # attribution data set. We want to drop those rows here because
        # otherwise if the data is further attributed (such as multiplied),
        # it could appear that data is dropped elsewhere when the dataset is
        # checked for null values
        fb = fb[fb['FlowAmount_other'] != 0].reset_index(drop=True)

        return (
            fb
            .drop(columns=['PrimarySector', 'SecondarySector',
                           'temp_location', 'denominator'],
                  errors='ignore')
            .drop(fb.filter(regex='_other').columns, axis=1)
            .reset_index(drop=True)
        )

    def multiplication_attribution(
        self: 'FB',
        other: 'FlowBySector'
    ) -> 'FlowByActivity':
        """
        This method takes flows from the calling FBA which are mapped to
        multiple sectors and multiplies them by flows from other (an FBS).
        """
        # determine units in each dataset
        self_units = self['Unit'].drop_duplicates().tolist()
        other_units = other['Unit'].drop_duplicates().tolist()

        log.info(f'Multiplying flows in {self.full_name} {self_units} by'
                 f' {other.full_name} {other_units}')
        fb_geoscale, other_geoscale, fb, other = self.harmonize_geoscale(
            other)

        if self.config.get('attribute_on') is not None:
            left_merge = self.config['attribute_on']
            right_merge = self.config['attribute_on']
        else:
            left_merge = ['PrimarySector', 'temp_location'
                                  if 'temp_location' in fb
                                  else 'Location']
            right_merge = ['PrimarySector', 'Location']

        # multiply using each dfs primary sector col
        merged = (fb
                  .merge(other,
                         how='left',
                         left_on=left_merge,
                         right_on=right_merge,
                         suffixes=[None, '_other'])
                  .fillna({'FlowAmount_other': 0})
                  )

        fill_col = self.config.get('fill_columns')
        if fill_col is not None:
            log.info(f'Replacing {fill_col} values in primary data source '
                     f'with those from attribution source.')
            merged[fill_col] = merged[f'{fill_col}_other']

        fb = (merged
              .assign(FlowAmount=lambda x: (x.FlowAmount
                                            * x.FlowAmount_other))
              .drop(columns=['PrimarySector_other', 'Location_other',
                             'FlowAmount_other', 'denominator'],
                    errors='ignore')
              )

        # determine if any flows are lost because multiplied by 0
        fb_null = fb[fb['FlowAmount'] == 0]
        if len(fb_null) > 0:
            # implode the location data to shorten warning message
            fb_null = fb_null.groupby(
                ['ActivityProducedBy', 'ActivityConsumedBy'], dropna=False,
                as_index=False).agg({'Location': lambda x: ", ".join(x)})
            log.warning('FlowAmounts in %s are reset to 0 due to lack of '
                        'flows in attribution source %s for '
                        'ActivityProducedBy/ActivityConsumedBy/Location: %s',
                        fb.full_name, other.full_name,
                        set(zip(fb_null.ActivityProducedBy,
                                fb_null.ActivityConsumedBy,
                                fb_null.Location))
                        )

            fb = fb[fb['FlowAmount'] != 0].reset_index(drop=True)

        # set new units, incorporating a check that units are correctly
        # converted
        rate = None
        if fb['Unit'].str.contains('/').all():
            rate = 'Unit'
            other = 'Unit_other'
        elif fb['Unit_other'].str.contains('/').all():
            rate = 'Unit_other'
            other = 'Unit'
        if rate is not None:
            fb['Denominator'] = fb[rate].str.split("/").str[1]
            fb[rate] = fb[rate].str.split("/").str[0]
            if fb[other].equals(fb['Denominator']) is False:
                log.warning('Check units being multiplied')
            else:
                log.info(f"Units reset to"
                         f" {fb[rate].drop_duplicates().tolist()}")
                fb['Unit'] = fb[rate] # updates when Unit_other is rate

        return (
            fb
            .drop(columns=['PrimarySector', 'SecondarySector',
                           'temp_location', 'group_total',
                           'Denominator'],
                  errors='ignore')
            .drop(fb.filter(regex='_other').columns, axis=1)
            .reset_index(drop=True)
        )

    def division_attribution(
        self: 'FB',
        other: 'FlowBySector'
    ) -> 'FlowByActivity':
        """
        This method takes flows from the calling FBA which are mapped to
        multiple sectors and divides them by flows from other (an FBS).
        """

        # determine units in each dataset
        self_units = self['Unit'].drop_duplicates().tolist()
        other_units = other['Unit'].drop_duplicates().tolist()

        log.info(f'Dividing flows in {self.full_name} {self_units} by'
                 f' {other.full_name} {other_units}')
        fb_geoscale, other_geoscale, fb, other = self.harmonize_geoscale(
            other)

        # divide using each dfs primary sector col
        merged = (fb
                  .merge(other,
                         how='left',
                         left_on=['PrimarySector', 'temp_location'
                                  if 'temp_location' in fb
                                  else 'Location'],
                         right_on=['PrimarySector', 'Location'],
                         suffixes=[None, '_other'])
                  .fillna({'FlowAmount_other': 0})
                  )

        fb = (merged
              .assign(FlowAmount=lambda x: (x.FlowAmount
                                            / x.FlowAmount_other))
              .drop(columns=['PrimarySector_other', 'Location_other',
                             'FlowAmount_other', 'denominator'],
                    errors='ignore')
              )

        # determine if any flows are lost because multiplied by 0
        fb_null = fb[fb['FlowAmount'] == 0]
        if len(fb_null) > 0:
            log.warning('FlowAmounts in %s are reset to 0 due to lack of '
                        'flows in attribution source %s for '
                        'ActivityProducedBy/ActivityConsumedBy/Location: %s',
                        fb.full_name, other.full_name,
                        set(zip(fb_null.ActivityProducedBy,
                                fb_null.ActivityConsumedBy,
                                fb_null.Location))
                        )

        # set new units
        fb['Unit'] = fb['Unit'] + '/' + fb['Unit_other']

        return (
            fb
            .drop(columns=['PrimarySector', 'SecondarySector',
                           'temp_location'],
                  errors='ignore')
            .reset_index(drop=True)
        )

    def equally_attribute(self: 'FB') -> 'FB':
        """
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
        """
        naics_key = naics.map_target_sectors_to_less_aggregated_sectors(
            self.config['industry_spec'], self.config['target_naics_year'])

        fba = self.add_primary_secondary_columns('Sector')

        groupby_cols = ['group_id', 'Location']
        for rank in ['Primary', 'Secondary']:
            # continue if values are all np.nan
            if fba[f'{rank}Sector'].isna().all():
                groupby_cols.append(f'{rank}Sector')
                continue

            fba = (
                fba
                .merge(naics_key, how='left', left_on=f'{rank}Sector',
                       right_on='target_naics')
                .assign(
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
                .drop(columns=naics_key.columns.values.tolist())
            )
            groupby_cols.append(f'{rank}Sector')

        return fba.drop(
            columns=['PrimarySector', 'SecondarySector',
                     *[f'_unique_naics_{n}_by_group' for n in range(2, 8)]]
        )

    def assign_temporal_correlation(
            self: FB,
            target_year = None,
            **kwargs
    ) -> FB:

        fbs = self.copy()
        if not target_year:
            target_year = int((re.search(r"\d{4}", fbs.full_name)).group())
        if 'TemporalCorrelation' not in fbs:
            fbs['TemporalCorrelation'] = 1
        fbs = esupy.dqi.adjust_dqi_scores(fbs, abs(fbs['Year'] - target_year),
                                         'TemporalCorrelation')
        fbs['Year'] = target_year
        return(fbs)

    def assign_geographic_correlation(
            self: FB,
            target_geoscale = None,
            fbs_method_name = None,
            **kwargs
    ) -> FB:

        fbs = self.copy()

        # if target_geoscale not assigned, pull target from FBS name
        if not target_geoscale:
            try:
                # function to extract target geoscale from fbs name
                def extract_target_geoscale(text):
                    # Match "national," "state," or "county" (case-insensitive)
                    if re.search(r"_national_", text, re.IGNORECASE):
                        return "national"
                    elif re.search(r"_state_", text, re.IGNORECASE):
                        return "state"
                    elif re.search(r"_county_", text, re.IGNORECASE):
                        return "county"
                    return None

                # if target geoscale not defined, first try pulling from config, else pull from method name
                target_geoscale = (self.config.get('target_geoscale') or
                                   extract_target_geoscale(fbs_method_name))
                if target_geoscale is None:
                    # if there isn't a target geoscale or method name, return FBS without appending geo correlation
                    return self
            except TypeError:
                return self

        log.info(f"Assigning geographical correlation data quality score for {self.full_name}")

        # if all Geo Corr scores in the FBS are 0, drop the column and reassign values
        if ('GeographicalCorrelation' in fbs and
            (fbs['GeographicalCorrelation'] == 0).all()):
            fbs = fbs.drop(columns=['GeographicalCorrelation'])

        # if Geo Corr column is missing from the df or if all Geo Corr column is all 0s, add score based on fips
        if 'GeographicalCorrelation' not in fbs:
            if fbs['LocationSystem'][0] == 'Census_Region':
                fbs = fbs.assign(GeographicalCorrelation = 4)
            elif fbs['LocationSystem'][0] == 'Census_Division':
                fbs = fbs.assign(GeographicalCorrelation = 3)
            else:
                # assign geo corr score by FIPS year
                try:
                    fips = (geo
                            .get_all_fips(int((re.search(r"\d{4}", fbs['LocationSystem'][0])).group()))
                            .rename(columns={'FIPS': 'Location',
                                             'FIPS_Scale':'GeographicalCorrelation'})
                            )
                # if FIPS year not defined, assume default year of 2015
                except AttributeError:
                    fips = (geo.get_all_fips()
                            .rename(columns={'FIPS': 'Location',
                                             'FIPS_Scale':'GeographicalCorrelation'})
                            )
                fbs = fbs.merge(fips[['Location', 'GeographicalCorrelation']])
        fbs['GeographicalCorrelation'] = fbs['GeographicalCorrelation'].astype(int)
        fbs['GeographicalCorrelation'] = fbs['GeographicalCorrelation'] - fips_number_key[target_geoscale]
        fbs = esupy.dqi.adjust_dqi_scores(fbs,
                                          fbs['GeographicalCorrelation'],
                                         'GeographicalCorrelation')
        return fbs

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
        fb = type(self)(df, convert_df_to_flowby=True, add_missing_columns=False, **metadata)

        return fb


"""
The three classes extending pd.Series, together with the _constructor...
methods of each class, are required for allowing pandas methods called on
objects of these classes to return objects of these classes, as desired.

For more information, see
https://pandas.pydata.org/docs/development/extending.html
"""
class _FlowBySeries(pd.Series):
    _metadata = [*_FlowBy()._metadata]

    @property
    def _constructor(self) -> '_FlowBySeries':
        return _FlowBySeries

    @property
    def _constructor_expanddim(self) -> '_FlowBy':
        return _FlowBy
