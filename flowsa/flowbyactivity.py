"""
FlowByActivity (FBA) data are attributed to a class, allowing the configuration
file and other attributes to be attached to the FBA object. The functions
defined in this file are specific to FBA data and.

Generation of FBA datasets calls on the functions defined in
gerneateflowbyactivity.py

"""
# necessary so 'FlowBySector'/'FlowByActivity' can be used in fxn
# annotations without importing the class to the py script which would lead
# to circular reasoning
from __future__ import annotations
from typing import TYPE_CHECKING

from functools import partial, reduce
from typing import Literal, List
import fedelemflowlist
import pandas as pd
import numpy as np

import flowsa.exceptions
from flowsa import settings, metadata, geo, validation, naics, common, \
    sectormapping, generateflowbyactivity
from flowsa.flowsa_log import log
from flowsa.settings import DEFAULT_DOWNLOAD_IF_MISSING
from flowsa.flowbyfunctions import filter_by_geoscale
from flowsa.flowby import _FlowBy, flowby_config, NAME_SEP_CHAR

if TYPE_CHECKING:
    from flowsa.flowbysector import FlowBySector

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
    def return_FBA(
        cls,
        full_name: str,
        year: int = None,
        git_version: str = None,
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

        # set metaname
        if year is None:
            meta_name = full_name
        else:
            meta_name = f'{full_name}_{year}'
        if git_version is not None:
            meta_name = f'{meta_name}_{git_version}'

        file_metadata = metadata.set_fb_meta(
            meta_name,
            'FlowByActivity'
        )
        flowby_generator = partial(
            generateflowbyactivity.main,
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
            log.error(f'Elementary flow list entries for {mapping_subset} not '
                      f'found')
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
            log.info(f'Units standardized to '
                     f'{list(mapping.TargetUnit.unique())} by mapping to '
                     f'federal elementary flow list')
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

        log.info(f'Determining appropriate source geoscale for '
                 f'{self.full_name}; target geoscale is '
                 f'{target_geoscale.name.lower()}')

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
            .assign(source_geoscale=
            fba_with_reporting_levels[reporting_level_columns].apply(
                lambda row: max((v for v in row if isinstance(v, geo.scale)), default=np.nan),
                axis=1))
            .query('geoscale == source_geoscale')
            .drop(columns=(['geoscale',
                            *geoscale_name_columns,
                            *reporting_level_columns]))
        ).reset_index(drop=True)

        if len(fba_at_source_geoscale.source_geoscale.unique()) > 1:
            log.warning(f"{fba_at_source_geoscale.full_name} has multiple "
                        f"source geoscales: "
                        f"{', '.join([s.name.lower() for s in fba_at_source_geoscale.source_geoscale.unique()])}")
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

        return fba_at_target_geoscale.reset_index(drop=True)

    def map_to_sectors(
            self: 'FlowByActivity',
            target_year: Literal[2002, 2007, 2012, 2017, 2022],
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
        from flowsa.flowbyclean import \
            define_parentincompletechild_descendants, \
            drop_parentincompletechild_descendants

        # determine activity schema and use for mapping
        activity_schema = self.config['activity_schema'] if isinstance(
            self.config['activity_schema'], str) else self.config.get(
            'activity_schema', {}).get(self.config['year'])

        if activity_schema is None:
            log.error(f"activity_schema is not defined, check assignment in flowsa/data/source_catalog.yaml")

        # Determine sector year
        if "NAICS" in activity_schema:
            log.info('Activities in %s are NAICS codes.',
                     self.full_name)
            try:
                source_year = int(activity_schema[6:10])
            except ValueError:
                source_year = 2017
                log.warning('No NAICS year given for NAICS activities in %s. '
                            '2012 used as default.', self.full_name)
            else:
                log.info('NAICS Activities in %s use NAICS year %s.',
                         self.full_name, source_year)

        # if FBA is not NAICS-like, pull sector year and crosswalk from activity to sector crosswalk
        else:
            log.info('Getting crosswalk between activities in %s and NAICS codes.', self.full_name)
            activity_to_source_naics_crosswalk = sectormapping.get_activitytosector_mapping(
                self.config.get('activity_to_sector_mapping') or self.source_name,
                fbsconfigpath=external_config_path
            ).astype('object')[['Activity', 'Sector', 'SectorType', 'SectorSourceName']]

            source_years = set(
                activity_to_source_naics_crosswalk.SectorSourceName
                .str.removeprefix('NAICS_')
                .str.removesuffix('_Code')
                .dropna().astype('int')
            )
            source_year = 2017 if 2017 in source_years else max(source_years) if source_years else 2017
            if not source_years:
                log.warning(
                    'No NAICS year/sector source name provided in crosswalk for %s. %s being used as default.',
                    self.full_name, source_year)

            activity_to_source_naics_crosswalk = activity_to_source_naics_crosswalk.query(
                f'SectorSourceName == "NAICS_{source_year}_Code"').reset_index(drop=True)

        # load the naics key for the source year - will convert to target year after mapping
        naics_key = naics.industry_spec_key(self.config['industry_spec'],
                                            source_year
                                            )
        # assign technological correlation scores
        naics_key = sectormapping.assign_technological_correlation(naics_key)

        # map the FBA activity columns to sectors
        fba_w_naics = self.copy()
        for direction in ['ProducedBy', 'ConsumedBy']:
            # skip if entire column is nan
            if fba_w_naics[f'Activity{direction}'].isna().all():
                fba_w_naics = (fba_w_naics
                               .assign(**{f"Sector{direction}": np.nan})
                               .assign(**{f"{direction}SectorType": np.nan})
                )
            else:
                if self.config.get('sector_hierarchy') == 'parent-incompleteChild':
                    # add descendants column
                    fba_w_naics = define_parentincompletechild_descendants(
                        fba_w_naics, activity_col=f'Activity{direction}')
                if "NAICS" in activity_schema:
                    primary_sector_key = naics_key
                    secondary_sector_key = None
                else:
                    primary_sector_key = activity_to_source_naics_crosswalk
                    secondary_sector_key = naics_key

                activity_to_target_naics_crosswalk = naics.subset_sector_key(
                    fba_w_naics,
                    f'Activity{direction}',
                    source_year,
                    primary_sector_key=primary_sector_key,
                    secondary_sector_key=secondary_sector_key
                )

                fba_w_naics = (fba_w_naics
                               .merge(activity_to_target_naics_crosswalk,
                                      how='left',
                                      on=['Class', 'Flowable', 'Context', 'ActivityProducedBy', 'ActivityConsumedBy'],
                                      )
                               .rename(columns={'target_naics': f'Sector{direction}', # when activities are sector-like
                                                'Sector': f'Sector{direction}', # when activities are text based
                                                'SectorType': f'{direction}SectorType'}
                                       )
                               .drop(columns=['ActivitySourceName',
                                              'SectorSourceName',
                                              'source_naics', # when activities are sector-like
                                              'Activity' # when activities are text based
                                              ], errors='ignore')
                               )
                # drop original DQ scores in favor of modified scores after mapping
                dq_cols = ['DataReliability', 'DataCollection']
                for c in dq_cols:
                    fba_w_naics.loc[fba_w_naics[f'{c}_y'].notnull(), f'{c}_x'] = fba_w_naics[f'{c}_y']
                    fba_w_naics = fba_w_naics.drop(columns=[f'{c}_y']).rename(columns={f'{c}_x': c})
                if fba_w_naics.config.get('sector_hierarchy') == 'parent-incompleteChild':
                    fba_w_naics = drop_parentincompletechild_descendants(fba_w_naics, sector_col=f'Sector{direction}')
        # assign data quality scores based on highest value, if there are data for both SCB and SPB
        for dq in ['DataReliability', 'DataCollection', 'TechnologicalCorrelation']:
            if f'{dq}_x' in fba_w_naics.columns:
                fba_w_naics = fba_w_naics.assign(
                    **{f'{dq}': fba_w_naics[[f'{dq}_x', f'{dq}_y']].apply(np.nanmax, axis=1)}
                )

        # convert NAICS year of data if necessary. Converting after mapping because we do not need to convert all
        # rows of data as not all rows of data are preserved. And in general if target sectors are NAICS3,
        # those data do not require conversion. For text-based activities, we need to convert to
        # sectors after mapping so we can apply the allocation ratio to the flow amount
        if source_year != self.config['target_naics_year']:
            # if FBA data are sector-like, convert the Activity column data to target sector year
            fba_w_naics = naics.convert_naics_year(
                fba_w_naics,
                f"NAICS_{fba_w_naics.config['target_naics_year']}_Code",
                f"NAICS_{source_year}_Code",
                self.full_name)

        # if activities are text-based, print out any data that are dropped
        if "NAICS" not in activity_schema:
            not_mapped = fba_w_naics[fba_w_naics[['SectorProducedBy', 'SectorConsumedBy']].isna().all(1)]

            if (len(not_mapped) > 0) & ("NAICS" not in activity_schema):
                not_mapped = not_mapped[['ActivityProducedBy', 'ActivityConsumedBy']].drop_duplicates()
                log.warning('Activities in %s are not mapped to sectors: %s', not_mapped.full_name, sorted(
                    set(not_mapped.ActivityProducedBy.dropna()).union(set(not_mapped.ActivityConsumedBy.dropna()))))

        # drop all NA data and clean up df
        fba_w_naics = fba_w_naics[
            ~(fba_w_naics['SectorProducedBy'].isna() & fba_w_naics['SectorConsumedBy'].isna())
        ]

        fba_w_naics2 = (fba_w_naics
                        .assign(SectorSourceName=f'NAICS_{target_year}_Code')
                       .drop(columns=['TechnologicalCorrelation_x', 'TechnologicalCorrelation_y',
                                      'DataReliability_x', 'DataReliability_y',
                                      'DataCollection_x', 'DataCollection_y'],
                             errors='ignore')
                       .reset_index(drop=True)
                       )

        return fba_w_naics2


    def prepare_fbs(
            self: 'FlowByActivity',
            external_config_path: str = None,
            download_sources_ok: bool = True,
            skip_select_by: bool = False,
            retain_activity_columns: bool = False,
            fbs_method_name: str = None,
            ) -> 'FlowBySector':

        from flowsa.flowbysector import FlowBySector

        # drop the activity columns in the FBS unless method yaml specifies
        # to keep them
        drop_cols = ['ActivityProducedBy', 'ActivityConsumedBy']
        if retain_activity_columns:
            drop_cols = []

        if 'activity_sets' in self.config:
            try:
                return (
                    pd.concat([
                        fba.prepare_fbs(
                            external_config_path=external_config_path,
                            download_sources_ok=download_sources_ok,
                            skip_select_by=True,
                            retain_activity_columns=retain_activity_columns,
                            fbs_method_name=fbs_method_name)
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
                return FlowBySector(pd.DataFrame(), convert_df_to_flowby=True)
        log.info(f'Processing FlowBySector for {self.full_name}')
        # Primary FlowBySector generation approach:
        return FlowBySector(
            self
            .function_socket('clean_fba_before_mapping')
            .select_by_fields(skip_select_by=skip_select_by)
            .function_socket('estimate_suppressed')
            .select_by_fields(skip_select_by=skip_select_by,
                              selection_fields=self.config.get(
                                  'selection_fields_after_data_suppression_estimation', 'null'))
            .convert_units_and_flows()  # and also map to flow lists
            .function_socket('clean_fba')
            .assign_geographic_correlation(fbs_method_name=fbs_method_name)
            .convert_to_geoscale()
            .attribute_flows_to_sectors(external_config_path=external_config_path,
                                        download_sources_ok=download_sources_ok)  # recursive call to prepare_fbs
            .drop(columns=drop_cols)
            .aggregate_flowby()
            .function_socket('clean_fbs_after_aggregation'),
            convert_df_to_flowby=True
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
        parent_fba = (self
                      .reset_index(names='row')
                      )

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
            if ((not child_fba.empty) and
                len(child_fba.query('FlowAmount != 0')) > 0):
                child_fba_list.append(child_fba.drop(columns='row'))
            else:
                log.error(f'Activity set {child_fba.full_name} is empty. '
                          'Check activity set definition!')

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
        if self.config.get('standardize_units', True):
            mapped = mapped.standardize_units()

        return mapped

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
                settings.datapath / f'{self.config["emissions_factors"]}.csv')
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


"""
The three classes extending pd.Series, together with the _constructor...
methods of each class, are required for allowing pandas methods called on
objects of these classes to return objects of these classes, as desired.

For more information, see
https://pandas.pydata.org/docs/development/extending.html
"""
class _FBASeries(pd.Series):
    _metadata = [*FlowByActivity()._metadata]

    @property
    def _constructor(self) -> '_FBASeries':
        return _FBASeries

    @property
    def _constructor_expanddim(self) -> 'FlowByActivity':
        return FlowByActivity


def getFlowByActivity(
        datasource: str,
        year: int,
        git_version: str = None,
        flowclass=None,
        geographic_level=None,
        download_FBA_if_missing=DEFAULT_DOWNLOAD_IF_MISSING
        ) -> pd.DataFrame:
    """
    Retrieves stored data in the FlowByActivity format
    :param datasource: str, the code of the datasource.
    :param year: int, a year, e.g. 2012
    :param flowclass: str or list, a 'Class' of the flow. Optional. E.g.
    'Water' or ['Employment', 'Chemicals']
    :param geographic_level: str, a geographic level of the data.
                             Optional. E.g. 'national', 'state', 'county'.
    :param download_FBA_if_missing: bool, if True will attempt to load from
        remote server prior to generating if file not found locally
    :return: a pandas DataFrame in FlowByActivity format
    """
    fba = FlowByActivity.return_FBA(
        full_name=datasource,
        config={},
        year=int(year),
        git_version=git_version,
        download_ok=download_FBA_if_missing
    )

    if len(fba) == 0:
        raise flowsa.exceptions.FBANotAvailableError(
            message=f"Error generating {datasource} for {str(year)}")
    if flowclass is not None:
        fba = fba.query('Class == @flowclass')
    # if geographic level specified, only load rows in geo level
    if geographic_level is not None:
        fba = filter_by_geoscale(fba, geographic_level)
    return pd.DataFrame(fba.reset_index(drop=True))
