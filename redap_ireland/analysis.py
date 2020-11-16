import os
import pandas as pd
import numpy as np
import synthpop.synthpop.zone_synthesizer as zs
import matplotlib.pyplot as plt
import contextily as ctx
import logging

import helper

logger = logging.getLogger('data_cleaning.analysis')

def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)
    percentile_.__name__ = 'percentile_%s' % n
    return percentile_

def analyse_ber_data(
        filename,
        output_dir,
        load_cleaned_data=False,
        generate_example_figures=False
):
    logger.info('Load cleaned data')
    data = pd.read_csv(filename, delimiter=';', header=0)


    # u-value statistics
    data['UValueWall'] = data['UValueWall'].replace(0, np.nan)
    data['UValueRoof'] = data['UValueRoof'].replace(0, np.nan)
    data['UValueFloor'] = data['UValueFloor'].replace(0, np.nan)
    data['UValueWindow'] = data['UValueWindow'].replace(0, np.nan)
    data['UvalueDoor'] = data['UvalueDoor'].replace(0, np.nan)

    u_values = data.groupby(['building_type', 'building_period']).agg({
        'UValueWall': [np.mean, np.std, np.median, np.var, np.min, np.max, percentile(10), percentile(90),'count'], 
        'UValueRoof': [np.mean, np.std, np.median, np.var, np.min, np.max, percentile(10), percentile(90),'count'], 
        'UValueFloor': [np.mean, np.std, np.median, np.var, np.min, np.max, percentile(10), percentile(90),'count'], 
        'UValueWindow': [np.mean, np.std, np.median, np.var, np.min, np.max, percentile(10), percentile(90),'count'], 
        'UvalueDoor': [np.mean, np.std, np.median, np.var, np.min, np.max, percentile(10), percentile(90),'count'], 
        })
    
    # heating system statistics
    heating_systems = data.groupby(['building_type', 'building_period', 'energy_carrier_space_heating', 'MainSpaceHeatingFuel', 'MainWaterHeatingFuel'])['index'].count()
    heating_system_efficiency = data.groupby(['building_period', 'MainSpaceHeatingFuel']).agg({
        'HSMainSystemEfficiency': ['mean', 'std', 'count'], 
        })
    hot_water_system_efficiency = data.groupby(['building_period', 'MainWaterHeatingFuel']).agg({
        'WHMainSystemEff': ['mean', 'std', 'count'], 
        })    
    
    # venilation system statistics
    ventilation_systems = data.groupby(['building_type', 'building_period', 'VentilationMethod'])['index'].count()
    
    writer = pd.ExcelWriter(
        os.path.join(output_dir, 'BER_data_analysis.xlsx')
    )
    u_values.to_excel(writer, sheet_name='u_values', index=True, merge_cells=False)
    heating_systems.to_excel(writer, sheet_name='heating_systems', index=True, merge_cells=False)
    heating_system_efficiency.to_excel(writer, sheet_name='heating_system_efficiency', index=True, merge_cells=False)
    hot_water_system_efficiency.to_excel(writer, sheet_name='hot_water_system_efficiency', index=True, merge_cells=False)
    ventilation_systems.to_excel(writer, sheet_name='ventilation_systems', index=True, merge_cells=False)
    writer.save()

    

