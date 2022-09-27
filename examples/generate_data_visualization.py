# generate_data_visualization.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Generate plots to explore Flow-By-Sector model outputs
"""

import flowsa
import flowsa.datavisualization as dv
from flowsa.settings import plotoutputpath
import matplotlib.pyplot as plt


########## Produce facet graph of resources associated with cropland sectors ##########
sectors = ['112']
sector_length_display = 6
plottype = 'facet_graph'
method_dict = {'Water Withdrawal 2015': 'Water_national_2015_m1',
               'Land Use 2012': 'Land_national_2012',
               'Employment 2017': 'Employment_national_2017'}


dv.FBSscatterplot(method_dict, plottype,
                       sector_length_display=sector_length_display,
                       sectors_to_include=sectors,
                       plot_title='Direct Resource Use for Livestock'
                       )
# Can manually adjust the figure pop up before saving
plt.savefig(f"{plotoutputpath}livestock_resource_use.png", dpi=300)


########## Compare the results between water method 1 and method 2 ##########
sectors = ['21']
sector_length_display = 6
plottype = 'method_comparison'
method_dict = {'Water Withdrawal 2015 M1': 'Water_national_2015_m1',
               'Water Withdrawal 2015 M2': 'Water_national_2015_m2'}

flowsa.generateFBSplot(method_dict, plottype,
                       sector_length_display=sector_length_display,
                       sectors_to_include=sectors,
                       plot_title='Comparison of 2015 National Water '
                                  'Withdrawals Method 1 and Method 2 for '
                                  'Mining Sectors'
                       )
# Can manually adjust the figure pop up before saving
plt.savefig(f"{plotoutputpath}mining_water_comp.png", dpi=300)


########## Compare food waste flows via Sankey ##########
methodnames = ['Food_Waste_national_2018_m1',
               'Food_Waste_national_2018_m2']
target_sector_level = 2
target_subset_sector_level = None
replace_SPB_with_sectordefinition = True
replace_SCB_with_sectordefinition = True
sectors_to_include = None
fbsconfigpath = None
orientation = 'horizontal'

dv.generateSankeyDiagram(
    methodnames,
    target_sector_level=target_sector_level,
    target_subset_sector_level=target_subset_sector_level,
    replace_SPB_with_sectordefinition=replace_SPB_with_sectordefinition,
    replace_SCB_with_sectordefinition=replace_SCB_with_sectordefinition,
    sectors_to_include=None,
    fbsconfigpath=None,
    orientation='orientation'
)
