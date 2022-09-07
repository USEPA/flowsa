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
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import flowsa
from flowsa.common import load_crosswalk, load_yaml_dict, \
    load_sector_length_cw_melt
from flowsa.dataclean import replace_NoneType_with_empty_cells
from flowsa.flowbyfunctions import sector_aggregation
from flowsa.sectormapping import get_sector_list
from flowsa.settings import log, datapath


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
                         usecols=[0,1], names=['Sector', 'Name'], skiprows=1
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
        else:
            cw = load_crosswalk('sector_name')
            cw['SectorName'] = cw['NAICS_2012_Code'].map(str) + ' (' + cw[
                'NAICS_2012_Name'] + ')'
            cw = cw.rename(columns={'NAICS_2012_Code': 'Sector'})

    # loop through sector columns, append sector name
    for s in sector_cols:
        df = df.merge(cw[['Sector', 'SectorName']],
                      left_on=s,
                      right_on='Sector',
                      how='left').drop(columns='Sector').rename(
            columns={'SectorName': f'{s}Name'})
        df[f'{s}Name'].fillna(df[s], inplace=True)
        df = df.reset_index(drop=True)

    return df


def FBSscatterplot(method_dict, plottype, sector_length_display=None,
                   sectors_to_include=None, plot_title=None):
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
    :return: graphic displaying results of FBS models
    """

    df_list = []
    for label, method in method_dict.items():
        dfm = flowsa.collapse_FlowBySector(method)
        if plottype == 'facet_graph':
            dfm['methodname'] = dfm['Unit'].apply(lambda x: f"{label} ({x})")
        elif plottype == 'method_comparison':
            dfm['methodname'] = label
        df_list.append(dfm)
    df = pd.concat(df_list, ignore_index=True)

    # subset df
    if sectors_to_include is not None:
        df = df[df['Sector'].str.startswith(tuple(sectors_to_include))]
    if sector_length_display is None:
        sector_length_display = df['Sector'].apply(lambda x: x.str.len()).max()
    df['Sector'] = df['Sector'].apply(lambda x: x[0:sector_length_display])
    df2 = df.groupby(['methodname', 'Sector', 'Unit'],
                     as_index=False).agg({"FlowAmount": sum})

    # load crosswalk and add names
    df3 = addSectorNames(df2)

    sns.set_style("whitegrid")

    # set plot title
    if plot_title is not None:
        title = plot_title
    else:
        title = ""

    if plottype == 'facet_graph':
        g = sns.FacetGrid(df3, col="methodname",
                          sharex=False, aspect=1.5, margin_titles=False)
        g.map_dataframe(sns.scatterplot, x="FlowAmount", y="SectorName")
        g.set_axis_labels("Flow Amount", "")
        g.set_titles(col_template="{col_name}")
        # adjust overall graphic title
        if plot_title is not None:
            g.fig.subplots_adjust(top=.8)
            g.fig.suptitle(title)
        g.tight_layout()

    elif plottype == 'method_comparison':
        g = sns.relplot(data=df3, x="FlowAmount", y="SectorName",
                        hue="methodname", alpha=0.7, style="methodname",
                        palette="colorblind",
                        aspect=1.5
                        ).set(title=title)
        g._legend.set_title('Flow-By-Sector Method')
        g.set_axis_labels(f"Flow Amount ({df3['Unit'][0]})", "")
        g.tight_layout()


def stackedBarChart(methodname, impact_cat=None):
    """
    Create a grouped, stacked barchart by sector code. If impact=True,
    group data by context as well as sector
    :param methodname: str, ex. "Water_national_m1_2015"
    :param impact_cat: str, name of impact category to apply and aggregate on
        impacts (e.g.: 'Global warming'). Use 'None' to aggregate by flow
    :return: stacked, group bar plot
    """

    df = flowsa.collapse_FlowBySector(methodname)

    index_cols = ["Location", "Sector", "Unit"]
    if impact_cat:
        try:
            import lciafmt
            df = (lciafmt.apply_lcia_method(df, 'TRACI2.1')
                  .rename(columns={'FlowAmount': 'InvAmount',
                                   'Impact': 'FlowAmount'}))
            var = 'Indicator'
            df = df[df['Indicator'] == impact_cat]
            if len(df) == 0:
                log.exception(f'Impact category: {impact_cat} not found')
                return
            df_unit = df['Indicator unit'][0]
        except ImportError:
            log.exception('lciafmt not installed')
            return
        except AttributeError:
            log.exception('check lciafmt branch')
            return
    else:
        # combine the flowable and context columns for graphing
        df['Flow'] = df['Flowable'] + ', ' + df['Context']
        var = 'Flow'
        df_unit = df['Unit'][0]
    index_cols = index_cols + [var]

    # If 'Allocationsources' value is null, replace with 'Direct
    df['AllocationSources'] = df['AllocationSources'].fillna('Direct')
    # aggregate by location/sector/unit and optionally 'context'
    df2 = df.groupby(index_cols + ['AllocationSources'],
                     as_index=False).agg({"FlowAmount": sum})
    df2 = df2.sort_values(['Sector', 'AllocationSources'])

    fig = go.Figure()

    fig.update_layout(
        template="simple_white",
        xaxis=dict(title_text=f"FlowAmount ({df_unit})"),
        yaxis=dict(title_text="Sector"),
        barmode="stack",
    )

    # create list of n colors based on number of allocation sources
    colors = px.colors.qualitative.Plotly[
             0:len(df2['AllocationSources'].unique())]

    for r, c in zip(df2['AllocationSources'].unique(), colors):
        plot_df = df2[df2['AllocationSources'] == r]
        y_axis_col = [plot_df['Sector'], plot_df[var]]
        fig.add_trace(
            go.Bar(x=plot_df['FlowAmount'],
                   y=y_axis_col, name=r,
                   orientation='h',
                   marker_color=c
                   ))

    fig.update_yaxes(autorange="reversed")
    fig.update_layout(title=methodname)

    # Render in browser
    # import plotly.io as pio
    # pio.renderers.default='browser'

    fig.show()


def plot_state_coefficients(fbs_coeff, indicator=None, sectors_to_include=None):
    from flowsa.location import get_state_FIPS, US_FIPS
    df = fbs_coeff.merge(get_state_FIPS(abbrev=True), how = 'left',
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
    g.set_axis_labels(f"{df['Indicator'][0]} ({df['Indicator unit'][0]} / $)", "")
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
                                df['FlowAmount']/(10**9),
                                df['FlowAmount'])
    df['Unit'] = np.where((df["Class"] == 'Other') & (df['Unit'] == 'kg'),
                          "MMT",
                          df['Unit'])

    return df


def generateSankeyData(methodname,
                       SPB_display_length=None,
                       SCB_display_length=None,
                       replace_SPB_with_sectordefinition=False,
                       replace_SCB_with_sectordefinition=False,
                       sectors_to_include=None,
                       fbsconfigpath=None):
    """
    Generate data used to create a sankey
    :param methodname: str, FBS methodname
    :param SPB_display_length: numeric, sector length by which to
    aggregate, default is 'None' which returns the max sector length in a
    dataframe
    :param _display_length: numeric, sector length by which to
    aggregate, default is 'None' which returns the max sector length in a
    dataframe
    :param sectors_to_include: list, sectors to include in output. Sectors
    are subset by all sectors that "start with" the values in this list
    :param fbsconfigpath, str, optional path to FBS method yaml
    :return: csv file for use in generating sankey diagram
    """

    df = flowsa.getFlowBySector(methodname)
    df = convert_units_for_graphics(df)

    # subset df
    if sectors_to_include is not None:
        df = df[df['Sector'].str.startswith(tuple(sectors_to_include))]

    # aggregate/subset to specified sectors to display
    method_dict = load_yaml_dict(methodname, flowbytype='FBS',
                                 filepath=fbsconfigpath)

    if any(item is not None for item in [SPB_display_length,
                                         SCB_display_length]):
        # aggregate to all sector levels
        df = sector_aggregation(df, return_all_possible_sector_combos=True)
        df = replace_NoneType_with_empty_cells(df)
        cw_load = load_sector_length_cw_melt()
        for s in ['Produced', 'Consumed']:
            if eval(f'S{s[0]}B_display_length') is not None:
                # subset the df by naics length
                cw = cw_load[cw_load['SectorLength'].isin([eval(
                    f'S{s[0]}B_display_length')])]
                sector_list = cw['Sector'].drop_duplicates().values.tolist()
                df = df[df[f'Sector{s}By'].isin(sector_list)]
            else:
                # subset by target sector levels
                secondary_sector_level = method_dict.get(
                    'target_subset_sector_level')
                sector_list = get_sector_list(
                    method_dict['target_sector_level'],
                    secondary_sector_level_dict=secondary_sector_level)
                df = df[df[f'Sector{s}By'].isin(sector_list)]
    # add sector names
    sankeymappingfile = f'{datapath}SankeyNames.csv'
    df2 = addSectorNames(df, mappingfile=sankeymappingfile)

    # create df for sankey diagram
    spb = df2[['SectorProducedBy', 'SectorProducedByName',
               'FlowAmount']].sort_values(['SectorProducedBy']).rename(
        columns={'SectorProducedBy': 'Nodes'})
    spb = spb.groupby(['Nodes', 'SectorProducedByName'], as_index=False)[
        'FlowAmount'].sum()
    spb['x_pos'] = 0.01
    # returns odd figure if set 0 - 1, so scale 0.01 to 0.99
    spb['y_pos'] = 0.01 + (spb.index * .98/(spb['Nodes'].count()-1))

    scb = df2[['SectorConsumedBy', 'SectorConsumedByName',
               'FlowAmount']].drop_duplicates().sort_values(
        ['SectorConsumedBy']).rename(columns={'SectorConsumedBy': 'Nodes'})
    scb = scb.groupby(['Nodes', 'SectorConsumedByName'], as_index=False)[
        'FlowAmount'].sum()
    scb['x_pos'] = .99
    # returns odd figure if set 0 - 1, so scale 0.01 to 0.99
    scb['y_pos'] = 0.01 + (scb.index * .98/(scb['Nodes'].count()-1))

    nodes = pd.concat([spb, scb], ignore_index=True)
    nodes['Num'] = nodes.index
    # add colors
    color_dict = load_yaml_dict('VisualizationColors')
    nodes['Color'] = nodes['Nodes'].map(color_dict)
    # fill in any colors missing from the color dictionary with random colors
    nodes['Color'] = nodes['Color'].fillna(
        "#%06x" % random.randint(0, 0xFFFFFF))

    if replace_SPB_with_sectordefinition:
        df2['SectorProducedBy'] = df2['SectorProducedByName']
        nodes['Nodes'] = np.where(~nodes['SectorProducedByName'].isna(),
                                  nodes['SectorProducedByName'],
                                  nodes['Nodes'])
    if replace_SCB_with_sectordefinition:
        df2['SectorConsumedBy'] = df2['SectorConsumedByName']
        nodes['Nodes'] = np.where(~nodes['SectorConsumedByName'].isna(),
                                  nodes['SectorConsumedByName'],
                                  nodes['Nodes'])
    nodes = nodes.drop(columns=['SectorProducedByName',
                                'SectorConsumedByName'])

    # add flow amounts to label
    nodes['Label'] = nodes['Nodes'] + '<br>' + nodes['FlowAmount'].round(
        2).astype(str)

    # subset df to sectors and flowamount
    flows = df2[['SectorProducedBy', 'SectorConsumedBy',
                 'FlowAmount', 'Unit']].rename(
        columns={'SectorProducedBy': 'Source',
                 'SectorConsumedBy': 'Target',
                 'FlowAmount': 'Value'})
    # add source and target numbers
    for c in ['Source', 'Target']:
        flows = flows.merge(nodes, left_on=c, right_on='Nodes').drop(
            columns='Nodes').rename(columns={'Num': f'{c}Num'})
    flows = flows[['SourceNum', 'Source', 'TargetNum', 'Target',
                   'Value', 'Unit']].sort_values(
        ['SourceNum', 'TargetNum']).reset_index(drop=True)

    return nodes, flows


def generateSankeyDiagram(methodnames,
                          SPB_display_length=None,
                          SCB_display_length=None,
                          replace_SPB_with_sectordefinition=False,
                          replace_SCB_with_sectordefinition=False,
                          sectors_to_include=None,
                          fbsconfigpath=None,
                          plot_title=None):
    """
    Sankey diagram developed to map flows between sector produced by (source)
    and sector consumed by (target). Sankey developed for subplot of 2
    diagrams.
    :param methodnames:
    :param SPB_display_length:
    :param SCB_display_length:
    :param replace_SPB_with_sectordefinition:
    :param replace_SCB_with_sectordefinition:
    :param sectors_to_include:
    :param fbsconfigpath:
    :param plot_title:
    :return:
    """

    fig = make_subplots(rows=1, cols=len(methodnames), shared_yaxes=True,
                        subplot_titles=methodnames)

    for i, m in enumerate(methodnames):
        # return dfs of nodes and flows for Sankey
        nodes, flows = generateSankeyData(
            m, SPB_display_length, SCB_display_length,
            replace_SPB_with_sectordefinition,
            replace_SCB_with_sectordefinition, sectors_to_include,
            fbsconfigpath)

        fig.add_trace(go.Sankey(
            arrangement="snap",
            domain={
                'x': [0 + (i/len(methodnames)) + 0.02,
                      ((i+1)/len(methodnames)) - 0.02]
            },
            valueformat=".1f",
            valuesuffix=flows['Unit'][0],
            # Define nodes
            node=dict(
                pad=15,
                thickness=15,
                line=dict(color="black", width=0.5),
                label=nodes['Label'].values.tolist(),
                color=nodes['Color'].values.tolist(),
                x=nodes['x_pos'].values.tolist(),
                y=nodes['y_pos'].values.tolist()
            ),
            # Add links
            link=dict(
                source=flows['SourceNum'].values.tolist(),
                target=flows['TargetNum'].values.tolist(),
                value=flows['Value'].values.tolist(),
                label=nodes['Nodes'].values.tolist(),
                # color=nodes['Color'].values.tolist()
            )))

    fig.update_layout(
        title_text=plot_title,
        font_size=10)

    fig.show()


if __name__ == '__main__':
    methodnames = ['Food_Waste_national_2018_m1',
                   'Food_Waste_national_2018_m2']
    SPB_display_length = 2
    SCB_display_length = None
    replace_SPB_with_sectordefinition=False
    replace_SCB_with_sectordefinition=True
    sectors_to_include = None
    fbsconfigpath = None

    generateSankeyDiagram(
        methodnames,
        SPB_display_length=SPB_display_length,
        SCB_display_length=SCB_display_length,
        replace_SPB_with_sectordefinition=replace_SPB_with_sectordefinition,
        replace_SCB_with_sectordefinition=replace_SCB_with_sectordefinition,
        sectors_to_include=None,
        fbsconfigpath=None
    )
