# datavisualization.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to plot Flow-By-Sector results
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import seaborn as sns
import flowsa
from flowsa.common import load_crosswalk
from flowsa.settings import log


def addSectorNames(df):
    """
    Add column to an FBS df with the sector names
    :param df: FBS df with singular "Sector" column
    :return: FBS df with new column of combined Sector and SectorNames
    """
    # load crosswalk and add names
    cw = load_crosswalk('sector_name')
    cw['SectorName'] = cw['NAICS_2012_Code'].map(str) + ' (' + cw[
        'NAICS_2012_Name'] + ')'
    cw = cw.rename(columns={'NAICS_2012_Code': 'Sector'})
    df = df.merge(cw[['Sector', 'SectorName']], how='left')
    df = df.reset_index(drop=True)

    return df


def plotFBSresults(method_dict, plottype, sector_length_display=None,
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
    :param impacts: str, name of impact category to apply and aggregate on
        impacts (e.g.: 'Global warming'). Use 'None' to aggregate by flow
    :return: stacked, group bar plot
    """

    df = flowsa.collapse_FlowBySector(methodname)
    df_unit = df['Unit'][0]

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
