# datavisualization.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to plot Flow-By-Sector results
"""

import pandas as pd
import numpy as np
import seaborn as sns
import random
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import flowsa
import flowsa.flowbysector
from flowsa.common import load_crosswalk, load_yaml_dict
# todo: need to update fxn to use new sector_aggregation - datavis not
#  currently working
# from flowsa.flowbyfunctions import sector_aggregation
from flowsa.flowsa_log import log
from flowsa.settings import datapath, plotoutputpath
import textwrap


def addSectorNames(df, BEA=False, mappingfile=None):
    """
    Add column to an FBS df with the sector names
    :param df: FBS df with singular "Sector" column or SectorProducedBy and
    SectorConsumedBy Cols
    :return: FBS df with new column of combined Sector and SectorNames
    """
    # determine which sector cols are in the df
    sector_cols = ['SectorProducedBy', 'SectorConsumedBy']
    if 'Sector' in df.columns:
        sector_cols = ['Sector']
    # load crosswalk and add names
    if BEA:
        cw = pd.read_csv('https://raw.githubusercontent.com/USEPA/useeior'
                         '/develop/inst/extdata/USEEIO_Commodity_Meta.csv',
                         usecols=[0, 1], names=['Sector', 'Name'], skiprows=1
                         )
        cw['SectorName'] = cw['Sector'] + ' (' + cw['Name'] + ')'
        # Limit length to 50 characters
        cw['SectorName'] = cw['SectorName'].str[:50]
        cw['SectorName'] = np.where(cw['SectorName'].str.len() == 50,
                                    cw['SectorName'] + '...)',
                                    cw['SectorName'])
    else:
        if mappingfile is not None:
            cw = pd.read_csv(mappingfile)
            cw = cw.rename(columns={'Abrv_Name': 'SectorName'})
        else:
            cw = load_crosswalk('Sector_2012_Names')
            cw['SectorName'] = cw['NAICS_2012_Code'].map(str) + ' (' + cw[
                'NAICS_2012_Name'] + ')'
            cw = cw.rename(columns={'NAICS_2012_Code': 'Sector'})

    # loop through sector columns, append sector name
    for s in sector_cols:
        df = df.merge(cw[['Sector', 'SectorName']],
                      left_on=s,
                      right_on='Sector',
                      how='left').rename(
            columns={'SectorName': f'{s}Name'})
        if s != 'Sector':
            df = df.drop(columns='Sector')
        df[f'{s}Name'].fillna(df[s], inplace=True)
        df = df.reset_index(drop=True)

    return df


def FBSscatterplot(method_dict,
                   plottype,
                   sector_length_display=None,
                   sectors_to_include=None,
                   impact_cat=None,
                   industry_spec=None,
                   sector_names=True,
                   legend_by_state=False,
                   legend_title='Flow-By-Sector Method',
                   axis_title=None,
                   plot_title=None):
    """
    Plot the results of FBS models. Graphic can either be a faceted
    scatterplot or a method comparison
    :param method_dict: dictionary, key is the label, value is the FBS
        methodname
    :param plottype: str, 'facet_graph' or 'method_comparison'
    :param sector_length_display: numeric, sector length by which to
    aggregate, default is 'None' which returns the max sector length in a
    dataframe
    :param sectors_to_include: list, sectors to include in output. Sectors
    are subset by all sectors that "start with" the values in this list
    :param impact_cat: str, name of impact category to apply and aggregate on
        impacts (e.g.: 'Global warming'), or dict in the form of
        {impact_method: impact_category}. Use 'None' to aggregate by flow
    :param industry_spec, dict e.g. {'default': 'NAICS_3',
                                     'NAICS_4': ['112', '113'],
                                     'NAICS_6': ['1129']}
    :param sector_names: bool, True to include sector names in axis
    :param legend_by_state: bool, True to show results by state
    :param axis_title: str
    :return: graphic displaying results of FBS models
    """

    df_list = []
    for label, method in method_dict.items():
        dfm = flowsa.flowbysector.collapse_FlowBySector(method)
        if plottype == 'facet_graph':
            dfm['methodname'] = dfm['Unit'].apply(lambda x: f"{label} ({x})")
        elif plottype in ('method_comparison', 'boxplot'):
            dfm['methodname'] = label
        df_list.append(dfm)
    df = pd.concat(df_list, ignore_index=True)

    if industry_spec is not None:
        # In order to reassign the industry spec, need to create and attach
        # the method config for subsequent functions to work
        df = flowsa.flowbysector.FlowBySector(df.reset_index(drop=True),
                                              config=load_yaml_dict(method, 'FBS'))
        # agg sectors for data visualization
        df.config['industry_spec'] = industry_spec
        # determine naics year in df
        df.config['target_naics_year'] = df['SectorSourceName'][0].split(
            "_", 1)[1].split("_", 1)[0]

        # Temporarily revert back to having both SPB and SCB, needed for
        # sector_aggregation()
        df = (df.rename(columns={'Sector':'SectorProducedBy'})
                .assign(SectorConsumedBy= np.nan))
        df = (df.sector_aggregation()
                .rename(columns={'SectorProducedBy':'Sector'})
                .drop(columns='SectorConsumedBy'))

    if legend_by_state:
        df = (df.merge(flowsa.location.get_state_FIPS(abbrev=True),
                       how='left',
                       left_on='Location',
                       right_on='FIPS')
                .drop(columns='methodname')
                .rename(columns={'State':'methodname'}))

    if impact_cat:
        if type(impact_cat)==str:
            imp_method = 'TRACI2.1'
            indicator = impact_cat
        else:
            imp_method = list(impact_cat.keys())[0]
            indicator = list(impact_cat.values())[0]
        try:
            import lciafmt
            df_impacts = (
                lciafmt.apply_lcia_method(df, imp_method)
                .rename(columns={'FlowAmount': 'InvAmount',
                                 'Impact': 'FlowAmount'}))
            # Handle special case of kg CO2e units not being converted but
            # should stay in the df
            fl = df.query('FlowUUID not in @df_impacts.FlowUUID')
            df_impacts = df_impacts.query('Indicator == @indicator')
            ind_units = set(df_impacts['Indicator unit'])
            # fl = fl.query('Unit in @ind_units')
            fl = fl.query('Unit == "kg CO2e"').assign(Unit = 'kg')
            df = pd.concat([df_impacts, fl], ignore_index=True)
            if len(df) == 0:
                log.exception(f'Impact category: {indicator} not found')
                return
        except ImportError:
            log.exception('lciafmt not installed')
            return
        except AttributeError:
            log.exception('check lciafmt branch')
            return
    df = convert_units_for_graphics(df)

    # subset df
    if sectors_to_include is not None:
        df = df[df['Sector'].str.startswith(tuple(sectors_to_include))]
    if sector_length_display is None:
        sector_length_display = df['Sector'].apply(lambda x: len(x)).max()
    df['Sector'] = df['Sector'].apply(lambda x: x[0:sector_length_display])
    df2 = df.groupby(['methodname', 'Sector', 'Unit'],
                     as_index=False).agg({"FlowAmount": sum})

    if sector_names:
        # load crosswalk and add names
        df2 = addSectorNames(df2)
        y_axis = "SectorName"
    else:
        y_axis = "Sector"

    sns.set_style("whitegrid")

    # set plot title
    if plot_title is not None:
        title = plot_title
    else:
        title = ""

    if plottype == 'facet_graph':
        if not axis_title:
            axis_title = 'Flow Amount'
        g = sns.FacetGrid(df2, col="methodname",
                          sharex=False, aspect=1.5, margin_titles=False)
        g.map_dataframe(sns.scatterplot, x="FlowAmount", y=y_axis)
        g.set_axis_labels(axis_title, "")
        g.set_titles(col_template="{col_name}")
        # adjust overall graphic title
        if plot_title is not None:
            g.fig.subplots_adjust(top=.8)
            g.fig.suptitle(title)
        g.tight_layout()

    elif plottype == 'method_comparison':
        if not axis_title:
            axis_title = f"Flow Amount ({df2['Unit'][0]})"
        g = sns.relplot(data=df2, x="FlowAmount", y=y_axis,
                        hue="methodname", alpha=0.7, style="methodname",
                        palette="colorblind",
                        # height=5,
                        aspect=1.5,
                        ).set(title=title)
        g._legend.set_title(legend_title)
        g.set_axis_labels(axis_title, "")
        g.tight_layout()
    elif plottype == 'boxplot':
        g = sns.boxplot(data=df2, x="FlowAmount", y=y_axis,
                        color="gray")
        g.set(xlabel = axis_title,
              ylabel = "")

    return g

def customwrap(s, width=30):
    return "<br>".join(textwrap.wrap(s, width=width))


def stackedBarChart(df,
                    impact_cat=None,
                    selection_fields=None,
                    industry_spec = None,
                    stacking_col='AttributionSources',
                    generalize_AttributionSources=False,
                    plot_title=None,
                    index_cols=None,
                    orientation='h',
                    grouping_variable=None,
                    sector_variable='Sector',
                    subplot=None,
                    subplot_order=None,
                    rows=1,
                    cols=1,
                    filename = 'flowsaBarChart',
                    axis_title = None,
                    graphic_width = 1200,
                    graphic_height = 1200
                    ):
    """
    Create a grouped, stacked barchart by sector code. If impact=True,
    group data by context as well as sector
    :param df: str or df, either an FBS methodname (ex. "Water_national_m1_2015") or a df
    :param impact_cat: str, name of impact category to apply and aggregate on
        impacts (e.g.: 'Global warming'), or dict in the form of
        {impact_method: impact_category}. Use 'None' to aggregate by flow
    :param industry_spec: dict e.g., {'default': 'NAICS_3',
                                     'NAICS_4': ['112', '113'],
                                     'NAICS_6': ['1129']}
    :param subplot_order: dict, e.g., {'Carbon dioxide': 1,
                                        'Methane': 2,
                                        'Nitrous oxide': 3,
                                        'Jobs': 4,
                                        'Wages': 5,
                                        'Taxes': 6
                                        }
    :return: stacked, group bar plot
    """
    # if the df provided is a string, load the fbs method, otherwise use the
    # df provided
    if (type(df)) == str:
        df = flowsa.flowbysector.FlowBySector.return_FBS(df)

    if generalize_AttributionSources:
        df['AttributionSources'] = np.where(
            df['AttributionSources'] != 'Direct',
            'Allocated',
            df['AttributionSources'])

    if selection_fields is not None:
        for k, v in selection_fields.items():
            df = df[df[k].str.startswith(tuple(v))]

    df = flowsa.flowbysector.FlowBySector(df.reset_index(drop=True))
    # agg sectors for data visualization
    if industry_spec is not None:
        df.config['industry_spec'] = industry_spec
    # determine naics year in df
    df.config['target_naics_year'] = df['SectorSourceName'][0].split(
        "_", 1)[1].split("_", 1)[0]

    df = df.sector_aggregation()
    # collapse the df, if the df is not already collapsed
    if 'Sector' not in df.columns:
        df = flowsa.flowbyfunctions.collapse_fbs_sectors(df)

    # convert units
    df = convert_units_for_graphics(df)

    if index_cols is None:
        index_cols = ["Location", "Sector", "Unit"]
    if impact_cat:
        if type(impact_cat)==str:
            imp_method = 'TRACI2.1'
            indicator = impact_cat
        else:
            imp_method = list(impact_cat.keys())[0]
            indicator = list(impact_cat.values())[0]
        try:
            import lciafmt
            df = (lciafmt.apply_lcia_method(df, imp_method)
                  .rename(columns={'FlowAmount': 'InvAmount',
                                   'Impact': 'FlowAmount'}))
            var = 'Indicator'
            df = df.query('Indicator == @indicator').reset_index(drop=True)
            df_unit = df['Indicator unit'][0]
            sort_cols = [sector_variable, stacking_col]
            if len(df) == 0:
                log.exception(f'Impact category: {indicator} not found')
                return
        except ImportError:
            log.exception('lciafmt not installed')
            return
        except AttributeError:
            log.exception('check lciafmt branch')
            return
    else:
        if grouping_variable is None:
            # combine the flowable and context columns for graphing
            df['Flow'] = df['Flowable'] + ', ' + df['Context']
            var = 'Flow'
        else:
            var = grouping_variable
        df_unit = df['Unit'][0]
        sort_cols = [sector_variable, var, stacking_col]
    index_cols = index_cols + [var]

    # if only single var in df, do not include in the graph on axis
    var_count = df[var].nunique()

    # If 'AttributionSources' value is null, replace with 'Direct
    try:
        df['AttributionSources'] = df['AttributionSources'].fillna('Direct')
    except KeyError:
        pass
    # aggregate by location/sector/unit and optionally 'context'
    df2 = df.groupby(index_cols + [stacking_col],
                     as_index=False).agg({"FlowAmount": sum})

    # determine list of subplots, use specified order if given in a dictionary
    try:
        primary_sort_col = [subplot]
        if subplot_order is not None:
            df2['order'] = df2[subplot].map(subplot_order)
            primary_sort_col = ['order']
        df2 = (df2
               .sort_values(primary_sort_col + [sector_variable, grouping_variable])
               .reset_index(drop=True)
               .drop(columns='order', errors='ignore')
               )
        plot_list = df2[subplot].drop_duplicates().values.tolist()
        # if subplot order specified, return list of dictionary keys
    except KeyError:
        plot_list = None

    # fill in non existent data with 0s to enable accurate sorting of sectors
    subset_cols = ['Location', 'Unit', var, subplot]
    flows = (df2[df2.columns.intersection(subset_cols)]
             .drop_duplicates()
             )
    # match all possible stacking variables
    flows[stacking_col] = [df[stacking_col].unique().tolist() for _ in range(len(flows))]
    flows = flows.explode(stacking_col)
    # match all possible sector variables
    flows[sector_variable] = [df[sector_variable].unique().tolist() for _ in range(len(flows))]
    flows = flows.explode(sector_variable)

    df2 = df2.merge(flows, how='outer')
    df2['FlowAmount'] = df2['FlowAmount'].fillna(0)
    # resort df
    if subplot is not None:
        df2[subplot] = pd.Categorical(df2[subplot], plot_list)
        df2 = df2.sort_values([subplot] + sort_cols).reset_index(drop=True)
    else:
        df2 = df2.sort_values(sort_cols).reset_index(drop=True)

    # wrap the sector col
    df2[sector_variable] = df2[sector_variable].apply(
        lambda x: customwrap(x, width=12))

    # establish subplots if necessary
    try:
        fig = make_subplots(rows=rows, cols=cols, subplot_titles=df2[
            subplot].drop_duplicates().values.tolist())
    except KeyError:
        fig = go.Figure()

    fig.update_layout(
        template="simple_white",
        barmode="stack",
    )

    # create list of n colors based on number of allocation sources
    colors = df2[[stacking_col]].drop_duplicates()
    # add colors
    vis = pd.read_csv(datapath / 'VisualizationEssentials.csv').rename(
        columns={'AttributionSource': stacking_col})
    colors = colors.merge(vis[[stacking_col, 'Color']], how='left')

    # fill in any colors missing from the color dictionary with random colors
    colors['Color'] = colors['Color'].apply(
        lambda x: x if pd.notnull(x) else "#%06x" % random.randint(0, 0xFFFFFF))
    # sort in reverse alphabetical order for the legend order
    colors = colors.sort_values([stacking_col], ascending=False).reset_index(drop=True)
    # merge back into df
    df2 = df2.merge(colors, how='left')

    if subplot is None:
        axis_title = axis_title or f"Flow Total ({df_unit})"
        for r, c in zip(colors[stacking_col], colors['Color']):
            plot_df = df2[df2[stacking_col] == r]
            y_axis_col = plot_df[sector_variable]
            # if there are more than one variable category add to y-axis
            if var_count > 1:
                y_axis_col = [plot_df[sector_variable], plot_df[var]]
            fig.add_trace(
                go.Bar(x=plot_df['FlowAmount'],
                        y=y_axis_col, name=r,
                        orientation='h',
                        marker_color=c
                        ))
        fig.update_xaxes(title_text=axis_title)
        fig.update_yaxes(title_text="Sector", tickmode='linear')
    else:
        s = 0
        for row in range(1, rows + 1):
            for col in range(1, cols + 1):
                df3 = pd.DataFrame(df2[df2[subplot] == plot_list[s]].reset_index(drop=True))
                axis_title = f"Flow Total ({df3['Unit'][0]})"
                for r, c in zip(colors[stacking_col], colors['Color']):
                    plot_df = df3[df3[stacking_col] == r].reset_index(drop=True)
                    flow_col = plot_df['FlowAmount']
                    sector_col = [plot_df[sector_variable], plot_df[var]]
                    if orientation == 'h':
                        x_data = flow_col
                        y_data = sector_col
                        xaxis_title = axis_title
                        yaxis_title = ""
                    else:
                        x_data = sector_col
                        y_data = flow_col
                        xaxis_title = ""
                        yaxis_title = axis_title
                    fig.add_trace(
                        go.Bar(x=x_data, y=y_data, name=r,
                                orientation=orientation,
                                marker_color=c,
                                ),
                        row=row,
                        col=col
                    )
                    fig.update_xaxes(title_text=xaxis_title, row=row, col=col)
                    fig.update_yaxes(title_text=yaxis_title, row=row, col=col)
                s = s + 1

    if orientation == 'h':
        fig.update_yaxes(autorange="reversed")
    fig.update_layout(title=plot_title,
                      autosize=False,
                      template="simple_white",
                      font_size=10, margin_b=150
                      )

    # prevent duplicate legend entries
    names = set()
    fig.for_each_trace(
        lambda trace:
        trace.update(showlegend=False)
        if (trace.name in names) else names.add(trace.name))

    fig.show()
    if filename is not None:
        log.info(f'Saving file to {plotoutputpath / filename}.svg')
        fig.write_image(plotoutputpath / f"{filename}.svg", width=graphic_width,
                        height=graphic_height)
    return fig


def plot_state_coefficients(fbs_coeff, indicator=None,
                            sectors_to_include=None):
    from flowsa.location import get_state_FIPS, US_FIPS
    df = fbs_coeff.merge(get_state_FIPS(abbrev=True), how='left',
                         left_on='Location', right_on='FIPS')
    df.loc[df['Location'] == US_FIPS, 'State'] = 'U.S.'
    if indicator is not None:
        df = df[df['Indicator'] == indicator]
    if sectors_to_include is not None:
        df = df[df['Sector'].str.startswith(tuple(sectors_to_include))]
    df = df.reset_index(drop=True)
    sns.set_style("whitegrid")
    if 'SectorName' in df:
        axis_var = 'SectorName'
    else:
        axis_var = 'Sector'
    g = (sns.relplot(data=df, x="Coefficient", y=axis_var,
                     hue="State", alpha=0.7, style="State",
                     palette="colorblind",
                     aspect=0.7, height=12)
         # .set(title="title")
         )
    g._legend.set_title('State')
    g.set_axis_labels(f"{df['Indicator'][0]} ({df['Indicator unit'][0]} / $)",
                      "")
    g.tight_layout()
    return g


def convert_units_for_graphics(df):
    """
    Convert units for easier display in graphics
    :param df:
    :return:
    """

    # convert kg to million metric tons for class other
    df['FlowAmount'] = np.where((df["Class"] == 'Other') &
                                (df['Unit'] == 'kg'),
                                df['FlowAmount'] / (10 ** 9),
                                df['FlowAmount'])
    df['Unit'] = np.where((df["Class"] == 'Other') & (df['Unit'] == 'kg'),
                          "MMT", df['Unit'])

    # convert kg to million metric tons for class chemicals
    df['FlowAmount'] = np.where((df["Class"] == 'Chemicals') &
                                (df['Unit'] == 'kg'),
                                df['FlowAmount'] / (10 ** 9),
                                df['FlowAmount'])
    df['Unit'] = np.where((df["Class"] == 'Chemicals') & (df['Unit'] == 'kg'),
                          "MMT", df['Unit'])

    # convert people to thousand
    df['FlowAmount'] = np.where(df['Unit'] == 'p',
                                df['FlowAmount'] / (1000),
                                df['FlowAmount'])
    df['Unit'] = np.where(df['Unit'] == 'p', "Thousand p", df['Unit'])

    return df

# todo: Need to update to work for recursive method - sector_aggregation()
# def generateSankeyData(methodname,
#                        target_sector_level=None,
#                        target_subset_sector_level=None,
#                        use_sectordefinition=False,
#                        sectors_to_include=None,
#                        fbsconfigpath=None,
#                        value_label_format='line_break'):
#     """
#     Generate data used to create a sankey
#     :param methodname: str, FBS methodname
#     :param target_sector_level: numeric, sector length by which to
#     aggregate, default is 'None' which returns the max sector length in a
#     dataframe
#     :param target_subset_sector_level: numeric, sector length by which to
#     aggregate, default is 'None' which returns the max sector length in a
#     dataframe
#     :param sectors_to_include: list, sectors to include in output. Sectors
#     are subset by all sectors that "start with" the values in this list
#     :param fbsconfigpath, str, optional path to FBS method yaml
#     :return: csv file for use in generating sankey diagram
#     """
#
#     df = flowsa.FlowBySector.return_FBS(
#         methodname, external_config_path=fbsconfigpath, download_sources_ok=True)
#
#     df = convert_units_for_graphics(df)
#
#     # subset df
#     if sectors_to_include is not None:
#         df = df[df['Sector'].str.startswith(tuple(sectors_to_include))]
#
#     method_dict = load_yaml_dict(methodname, flowbytype='FBS',
#                                  filepath=fbsconfigpath)
#
#     if any(item is not None for item in [target_sector_level,
#                                          target_subset_sector_level]):
#         # subset by target sector levels, either those defined in
#         # function call or in method_dict
#         primary_sector_level = target_sector_level
#         if primary_sector_level is None:
#             primary_sector_level = method_dict['target_sector_level']
#         # pull secondary sector level by methodname if specified, otherwise
#         # use singular sector level
#         try:
#             secondary_sector_level = target_subset_sector_level[methodname]
#         except KeyError:
#             secondary_sector_level = target_subset_sector_level
#         if secondary_sector_level is None:
#             secondary_sector_level = method_dict.get(
#                 'target_subset_sector_level')
#         # check if different rules for sector columns
#         if any([s in secondary_sector_level for s in
#                 ['SectorProducedBy', 'SectorConsumedBy']]):
#             sector_list = {}
#             for s in ['Produced', 'Consumed']:
#                 try:
#                     sectors = get_sector_list(
#                         primary_sector_level,
#                         secondary_sector_level_dict=secondary_sector_level[f'Sector{s}By'])
#                     sector_list[f'Sector{s}By'] = sectors
#                 except KeyError:
#                     sectors = get_sector_list(
#                         primary_sector_level,
#                         secondary_sector_level_dict=None)
#                     sector_list[f'Sector{s}By'] = sectors
#         else:
#             sector_list = get_sector_list(
#                 primary_sector_level,
#                 secondary_sector_level_dict=secondary_sector_level)
#
#         # aggregate to all sector levels
#         df = sector_aggregation(df, return_all_possible_sector_combos=True,
#                                 sectors_to_exclude_from_agg=sector_list)
#         df = replace_NoneType_with_empty_cells(df)
#         for s in ['Produced', 'Consumed']:
#             if isinstance(sector_list, dict):
#                 df = df[df[f'Sector{s}By'].isin(sector_list[f'Sector{s}By'])]
#             else:
#                 df = df[df[f'Sector{s}By'].isin(sector_list)]
#
#     # add sector names
#     sankeymappingfile = datapath / 'VisualizationEssentials.csv'
#     df2 = addSectorNames(df, mappingfile=sankeymappingfile)
#
#     # subset df and aggregate flows by sectors
#     df2 = df2[['SectorProducedBy', 'SectorConsumedBy', 'FlowAmount', 'Unit',
#                'SectorProducedByName', 'SectorConsumedByName']]
#     df3 = df2.groupby(['SectorProducedBy', 'SectorConsumedBy', 'Unit',
#                        'SectorProducedByName', 'SectorConsumedByName']
#                       )['FlowAmount'].agg('sum').reset_index()
#
#     # define which columns to use as source and target
#     if use_sectordefinition:
#         categories = ['SectorProducedByName', 'SectorConsumedByName']
#         sources = 'SectorProducedByName'
#         targets = 'SectorConsumedByName'
#         sector_col = 'SectorName'
#     else:
#         categories = ['SectorProducedBy', 'SectorConsumedBy']
#         sources = 'SectorProducedBy'
#         targets = 'SectorConsumedBy'
#         sector_col = 'Sector'
#
#     # sort df by categories to help order how sectors appear in sankey
#     df3 = df3.sort_values(categories).reset_index(drop=True)
#
#     # create new df with node information
#     nodes = pd.DataFrame(
#         {'Sector': list(pd.unique(
#             df3[['SectorProducedBy', 'SectorConsumedBy']].values.ravel('K'))),
#          'SectorName': list(
#              pd.unique(df3[['SectorProducedByName',
#                             'SectorConsumedByName']].values.ravel('K')))
#          })
#
#     # add colors
#     vis = pd.read_csv(datapath / 'VisualizationEssentials.csv')
#     nodes = nodes.merge(vis[['Sector', 'Color']], how='left')
#     # fill in any colors missing from the color dictionary with random colors
#     nodes['Color'] = nodes['Color'].apply(lambda x: x if pd.notnull(x) else
#     "#%06x" % random.randint(0, 0xFFFFFF))
#     nodes = nodes.rename(columns={sector_col: 'node'})
#     # add label
#     # determine flow amount labels - sum incoming and outgoing flow amounts,
#     # use outgoing flow values when exist, otherwise incoming flow totals
#     outgoing = df3[[sources, 'FlowAmount']]
#     outgoing = outgoing.groupby([sources]).agg(
#         {'FlowAmount': 'sum'}).reset_index()
#     outgoing = outgoing.rename(columns={sources: 'node',
#                                         'FlowAmount': 'outgoing'})
#     incoming = df3[[targets, 'FlowAmount']]
#     incoming = incoming.groupby([targets]).agg(
#         {'FlowAmount': 'sum'}).reset_index()
#     incoming = incoming.rename(columns={targets: 'node',
#                                         'FlowAmount': 'incoming'})
#     flow_labels = outgoing.merge(incoming, how='outer')
#     flow_labels = (flow_labels.fillna(0)
#                    .assign(flow=np.where(flow_labels['outgoing'] > 0,
#                                          flow_labels['outgoing'],
#                                          flow_labels['incoming'])))
#     if value_label_format == 'line_break':
#         flow_labels['Label'] = flow_labels['node'] + '<br>' + \
#                                flow_labels['flow'].round(2).astype(str)
#     elif value_label_format == 'brackets':
#         flow_labels['Label'] = flow_labels['node'] + ' (' + \
#                                flow_labels['flow'].round(2).astype(str) + ')'
#     nodes = nodes.merge(flow_labels[['node', 'Label']], how='left')
#
#     # create flow dataframe where source and target are converted to numeric
#     # indices
#     flows = pd.DataFrame()
#     label_list = list(pd.unique(df3[categories].values.ravel('K')))
#     flows['source'] = df3[sources].apply(lambda x: label_list.index(x))
#     flows['target'] = df3[targets].apply(lambda x: label_list.index(x))
#     flows['value'] = df3['FlowAmount']
#     flows['Unit'] = df3['Unit']
#
#     return nodes, flows

# todo: comment back in once reformatted for recursive method
# def generateSankeyDiagram(methodnames,
#                           target_sector_level=None,
#                           target_subset_sector_level=None,
#                           use_sectordefinition=False,
#                           sectors_to_include=None,
#                           fbsconfigpath=None,
#                           plot_title=None,
#                           orientation='horizonal',
#                           domain_dict=None,
#                           plot_dimension=None,
#                           value_label_format='line_break',
#                           subplot_titles=None,
#                           filename='flowsaSankey'):
#     """
#     Sankey diagram developed to map flows between sector produced by (source)
#     and sector consumed by (target). Sankey developed for subplot of 2
#     diagrams.
#     :param methodnames:
#     :param target_sector_level:
#     :param target_subset_sector_level:
#     :param replace_SPB_with_sectordefinition:
#     :param replace_SCB_with_sectordefinition:
#     :param sectors_to_include:
#     :param fbsconfigpath:
#     :param plot_title:
#     :param orientation:
#     :param domain_dict: dict, manually set x and y coordinates of each subplot
#     :param plot_dimension: list, [width, height]
#     :param value_label_format: string, either 'line_break' or 'brackets'
#     :param subplot_titles: list, subplot titles
#     :return:
#     """
#     if orientation == 'vertical':
#         rows = len(methodnames)
#         cols = 1
#     else:
#         rows = 1
#         cols = len(methodnames)
#
#     fig = make_subplots(rows=rows, cols=cols, shared_yaxes=True,
#                         subplot_titles=subplot_titles)
#
#     for i, m in enumerate(methodnames):
#         # return dfs of nodes and flows for Sankey
#         nodes, flows = generateSankeyData(
#             m, target_sector_level, target_subset_sector_level,
#             use_sectordefinition, sectors_to_include,
#             fbsconfigpath, value_label_format=value_label_format)
#
#         # define domain
#         if domain_dict is None:
#             if orientation == 'vertical':
#                 domain = {'x': [0, 1],
#                           'y': [0 + (i / len(methodnames)) + 0.04,
#                                 ((i + 1) / len(methodnames)) - 0.04]}
#             else:
#                 domain = {'x': [0 + (i / len(methodnames)) + 0.02,
#                                 ((i + 1) / len(methodnames)) - 0.02],
#                           'y': [0, 1]}
#         else:
#             domain = {'x': domain_dict[i]['x'],
#                       'y': domain_dict[i]['y']}
#
#         fig.add_trace(go.Sankey(
#             arrangement="snap",
#             domain=domain,
#             valueformat=".1f",
#             valuesuffix=flows['Unit'][0],
#             # Define nodes
#             node=dict(
#                 pad=15,
#                 thickness=15,
#                 line=dict(color="black", width=0.5),
#                 label=nodes['Label'].values.tolist(),
#                 color=nodes['Color'].values.tolist(),
#             ),
#             # Add links
#             link=dict(source=flows['source'].values.tolist(),
#                       target=flows['target'].values.tolist(),
#                       value=flows['value'].values.tolist(),
#             ),
#         ))
#
#     fig.update_layout(
#         title_text=plot_title, font_size=10, margin_b=150)
#
#     if plot_dimension is None:
#         if orientation == 'vertical':
#             width = 1400
#             height = 1600
#         else:
#             width = 1100
#             height = 900
#     else:
#         width = plot_dimension[0]
#         height = plot_dimension[1]
#
#     fig.show()
#     log.info(f'Saving file to {plotoutputpath / filename}.svg')
#     fig.write_image(plotoutputpath / f"{filename}.svg",
#                     width=width, height=height)
