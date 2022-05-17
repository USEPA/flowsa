# datavisualization.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to plot Flow-By-Sector results
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import flowsa
from flowsa.common import load_crosswalk
from flowsa.flowbyfunctions import subset_df_by_sector_lengths


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


def stackedBarChart(methodname):

    # test
    methodname = 'Water_national_2015_m1'

    df = flowsa.collapse_FlowBySector(methodname)
    df['Sector'] = df['Sector'].apply(lambda x: x[0:2])
    df2 = df.groupby(['Location', 'Sector', 'Unit', 'DataSources'],
                     as_index=False).agg({"FlowAmount": sum})
    df3 = pd.pivot_table(df2, values="FlowAmount",
                         index=["Location", "Sector", "Unit"],
                         columns="DataSources", fill_value=0).reset_index()

    # plot the dataframe with 1 line
    ax = df3.plot.barh(x='Sector', stacked=True, figsize=(8, 6))

    # .patches is everything inside of the chart
    for rect in ax.patches:
        # Find where everything is located
        height = rect.get_height()
        width = rect.get_width()
        x = rect.get_x()
        y = rect.get_y()

        # The height of the bar is the data value and can be used as the label
        label_text = f'{width:.2f}%'  # f'{width:.2f}' to format decimal values

        # ax.text(x, y, text)
        label_x = x + width / 2
        label_y = y + height / 2

        # only plot labels greater than given width
        if width > 0:
            ax.text(label_x, label_y, label_text, ha='center', va='center',
                    fontsize=8)

    # move the legend
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)

    # add labels
    # ax.set_ylabel("People", fontsize=18)
    # ax.set_xlabel("Percent", fontsize=18)
    plt.show()












