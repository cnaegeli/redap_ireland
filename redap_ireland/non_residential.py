"""
process
- load and add data
- aggregate to buildings and asign id
- define building type based on main usage
- non-res usage classification
- define number of stories and size of the building based usage data 
- generate geometry
- generate cityjson file

problem: mix usage?

"""
import os
import pandas as pd
import numpy as np
import geopandas as gpd
import helper
import logging

import contextily as ctx
import matplotlib.pyplot as plt

logger = logging.getLogger('data_cleaning.non_residential')

def load_and_clean_data(filenames_data, filename_categories):
    use_categories = pd.read_csv(filename_categories, delimiter=";", header=[0], index_col=[0,1])
    data = pd.DataFrame()
    for key, filename in filenames_data.items():
        new_data = pd.read_csv(filename, delimiter=",", header=[0], encoding="ISO-8859-1")
        new_data['city_district'] = key

        new_data.drop(new_data[new_data.Category == "CHECK CATEGORY"].index, inplace=True)
        new_data.drop(new_data[new_data.Category == "CENTRAL VALUATION LIST"].index, inplace=True)
        new_data.drop(new_data[new_data.Category == "NON-LIST"].index, inplace=True)
        new_data.drop(new_data[new_data.Category == "NO CATEGORY SELECTED"].index, inplace=True)
        new_data.drop(new_data[new_data.Uses == "DEMOLISHED / INCAPABLE OF USE, -"].index, inplace=True)


        new_data.drop(new_data[new_data.Area == 0].index, inplace=True)
        new_data.dropna(subset=['Area'], inplace=True)

        data = data.append(new_data, ignore_index=True)
        
    data.dropna(subset=[' X ITM'], inplace=True)
    data.dropna(subset=[' Y ITM'], inplace=True)
    data = data[data[' X ITM']!=0]
    data = data[data[' Y ITM']!=0]
    
    data = data[data['Area']>0]
    #data = data[data['Area']<10000] # TODO: Clean error in data
    
    # split uses
    data["Uses"] = data["Uses"].str.replace("-, -", "")
    data["Uses"] = data["Uses"].str.replace("-, ", "")
    data["Uses"] = data["Uses"].str.replace(", -", "")
    data = data[data['Uses']!='-']
    data[['use_1', 'use_2', 'use_3', 'use_4']] = data["Uses"].str.split(", ", expand=True)
    
    data.dropna(subset=['use_1'], inplace=True)
    data = data[data['use_1']!=""] # TODO: Assign use based on category
    
    # remove not needed uses
    data = data.merge(use_categories, left_on=['Category', 'use_1'], right_index=True, how='left')
    data = data[data['Relevant']]

    # split level information
    data["Level"] = data["Level"].str.replace("0-1", "0,1")
    data["Level"] = data["Level"].str.replace("0-2", "0,2")
    data["Level"] = data["Level"].str.replace("0-3", "0,3")
    data["Level"] = data["Level"].str.replace("0-4", "0,4")
    data["Level"] = data["Level"].str.replace("0-5", "0,5")
    data["Level"] = data["Level"].str.replace("0-6", "0,6")
    data["Level"] = data["Level"].str.replace("0-7", "0,7")
    data["Level"] = data["Level"].str.replace("1-2", "1,2")
    data["Level"] = data["Level"].str.replace("1-4", "1,4")
    data["Level"] = data["Level"].str.replace("2-3", "2,3")
    data["Level"] = data["Level"].str.replace("0+1", "0,1", regex=False)
    data["Level"] = data["Level"].str.replace("1+2", "1,2", regex=False)
    data["Level"] = data["Level"].str.replace("1/2'", "1,2")
    data["Level"] = data["Level"].str.replace(".", ",")
    data["Level"] = data["Level"].str.replace(", ", ",")
    data["Level"] = data["Level"].str.replace("rtn", "")
    data["Level"] = data["Level"].str.replace("RTN", "")
    data["Level"] = data["Level"].str.replace("Rtn", "")
    data["Level"] = data["Level"].str.replace("Rtn", "")
    data["Level"] = data["Level"].str.replace("st", "")
    data["Level"] = data["Level"].str.replace("GF", "0")
    data["Level"] = data["Level"].str.replace("Mezzanine", "0")
    data["Level"] = data["Level"].str.replace("MEZZ", "0")
    data["Level"] = data["Level"].str.replace("Mezz", "0")
    data["Level"] = data["Level"].str.replace("mezz", "0")
    data["Level"] = data["Level"].str.replace("M", "0")
    data["Level"] = data["Level"].str.replace("o", "0")
    data["Level"] = data["Level"].str.replace("O", "0")
    data["Level"] = data["Level"].str.replace("attic", "")
    data["Level"] = data["Level"].str.replace("Attic", "")
    data["Level"] = data["Level"].str.replace(",a", "")
    data["Level"] = data["Level"].str.replace(",b", "")
    data["Level"] = data["Level"].str.replace("555", "5")
    data["Level"] = data["Level"].str.replace("  ", ",")
    data["Level"] = data["Level"].str.lstrip('GE')
    data["Level"] = data["Level"].str.strip()
    data["Level"] = data["Level"].str.rstrip(',')
    data["Level"] = data["Level"].str.lstrip(',')
    data["Level"] = data["Level"].where(data["Level"]!="", '0')
    data["Level"] = data["Level"].fillna('0')
        
    def get_min(levels):
        levels = list(map(int, levels))
        return min(levels)
    def get_max(levels):
        levels = list(map(int, levels))
        return max(levels)
    data['min_floor_level'] = data["Level"].str.split(",").map(get_min)
    data['max_floor_level'] = data["Level"].str.split(",").map(get_max)

    #rename and remove columns
    rename_columns = {
        'Property Number': 'usage_id',
        ' County': 'county',
        ' Local Authority': 'local_authority',
        'Category': 'category',
        'Uses': 'uses',
        'use_1': 'use_1',
        'use_2': 'use_2',
        'use_3': 'use_3',
        'use_4': 'use_4',
        'BSMUseClass': 'building_use_type',
        'BSMBuildingTypes': 'building_type',
        ' X ITM': 'x_coord',
        ' Y ITM': 'y_coord',
        'Level': 'floor_levels',
        'min_floor_level': 'min_floor_level',
        'max_floor_level': 'max_floor_level',
        'Floor Use': 'floor_use',
        'Area': 'floor_area',
    }
    data = data[rename_columns.keys()]
    data.rename(columns=rename_columns, inplace=True)
    data.reset_index(inplace=True)
    
    return data

def generate_stock(
    filenames,
    output_dir,
    load_cleaned_data=False,
    generate_example_figures=False,
):
    
    if load_cleaned_data:
        logger.info('Load cleaned data')
        non_res_data = pd.read_csv(filenames['non_res_data_cleaned'], delimiter=';', header=0)
    else:
        logger.info('Load and clean data')
        non_res_data = load_and_clean_data(filenames['orig_files'], filenames['non_res_use_categories'])
        non_res_data.to_csv(filenames['non_res_data_cleaned'], sep=';', header=True)

    small_area_data = pd.read_csv(filenames['small_area_data_cleaned'], delimiter=';', header=[0,1], index_col=[0])
    small_areas = helper.load_geo_data(filenames['small_area_geodata'])

    logger.info('define building data')
    buildings = non_res_data.groupby(['x_coord', 'y_coord']).agg({
        'county': 'first', 
        'local_authority': 'first', 
        'building_type': 'first', 
        'min_floor_level': 'min', 
        'max_floor_level': 'max', 
        'floor_area': 'sum', 
        })
    buildings.reset_index(drop=False, inplace=True)
    buildings['building_id'] = buildings.index
    buildings['number_of_floors'] = buildings['max_floor_level'] - buildings['min_floor_level'] + 1
    
    buildings['number_of_floors_below_ground'] = -buildings['min_floor_level']
    buildings['number_of_floors_below_ground'] = buildings['number_of_floors_below_ground'].where(
        buildings["number_of_floors_below_ground"]>0, 0
        )
    buildings['number_of_floors_above_ground'] = buildings['number_of_floors'] - buildings['number_of_floors_below_ground']
    
    logger.info('define building usage data')
    building_usages = non_res_data.groupby('usage_id').agg({
        'county': 'first', 
        'local_authority': 'first', 
        'category': 'first', 
        'uses': 'first', 
        'use_1': 'first', 
        'building_use_type': 'first',
        'building_type': 'first',
        'x_coord': 'first', 
        'y_coord': 'first', 
        'floor_area': 'sum', 
        })
    building_usages['usage_id'] = building_usages.index
    building_usages = building_usages.merge(
        buildings[['building_id', 'x_coord', 'y_coord']],
        left_on=['x_coord', 'y_coord'], 
        right_on=['x_coord', 'y_coord'],
        how='left'
        )
    
    logger.info('Map buildings to small areas')
    buildings = gpd.GeoDataFrame(
        buildings,
        geometry=gpd.points_from_xy(buildings['x_coord'], buildings['y_coord']),
        crs="EPSG:2157",
    )
    buildings = gpd.sjoin(buildings, small_areas[['area_id','geometry']], how="inner", op='within')

    logger.info('Complete building data based on small area')
    relevant_small_areas = list(buildings.groupby('area_id').count().index)
    small_area_data = small_area_data[small_area_data.index.isin(relevant_small_areas)]
    
    building_period = small_area_data.loc[: ,('building_period',)]
    def sample_building_period(area_id):
        distribution = building_period.loc[area_id,]
        bp = distribution.sample(1, weights=distribution, replace=True)
        bp = bp.index[0]
        return bp
    buildings['building_period'] = buildings['area_id'].map(sample_building_period)
    
    energy_carrier_space_heating = small_area_data.loc[: ,('energy_carrier_space_heating',)]
    # TODO: check if feasible
    del energy_carrier_space_heating['No central heating']
    del energy_carrier_space_heating['LPG']
    del energy_carrier_space_heating['Other']
    del energy_carrier_space_heating['Peat']
    def sample_energy_carrier(area_id):
        distribution = energy_carrier_space_heating.loc[area_id,]
        energy_carrier = distribution.sample(1, weights=distribution, replace=True)
        energy_carrier = energy_carrier.index[0]
        return energy_carrier
    buildings['energy_carrier_space_heating'] = buildings['area_id'].map(sample_energy_carrier)
    
    logger.info('Write output data')
    buildings.to_csv(
        os.path.join(output_dir, 'Dublin_Non_Residential_Buildings.csv'), sep=';', header=True, index=True
    )
    building_usages.to_csv(
        os.path.join(output_dir, 'Dublin_Non_Residential_Building_Usages.csv'), sep=';', header=True, index=True
    )
    
    
    
    if generate_example_figures:
        logger.info('Generate example maps')
        floor_area = buildings.groupby('area_id')['floor_area'].sum()
        floor_area = small_areas.merge(floor_area, left_on='area_id', right_index=True, how='left')
        floor_area = floor_area.to_crs(epsg=3857)
        floor_area['floor_area_per_ha'] = floor_area['floor_area'] / floor_area.geometry.area
        
        fig, ax = plt.subplots(1, figsize=(12, 12))
        floor_area.plot(
            column='floor_area',
            ax=ax,
            cmap='RdBu_r',
            alpha = 0.75,
            legend=True,
            #scheme="quantiles",
            #classification_kwds={'k':5},
            #legend_kwds={'label': "Energy demand [MWh/year]"},
            # missing_kwds={'color': 'lightgrey'},
            # edgecolor='black',
        )
        ctx.add_basemap(ax, source=ctx.providers.CartoDB.Voyager)
        fig.savefig(os.path.join(output_dir, "Non_residential_floor_area.png"))
        
        fig, ax = plt.subplots(1, figsize=(12, 12))
        floor_area.plot(
            column='floor_area_per_ha',
            ax=ax,
            cmap='RdBu_r',
            alpha = 0.75,
            legend=True,
            #scheme="quantiles",
            #classification_kwds={'k':5},
            #legend_kwds={'label': "Energy demand [MWh/year]"},
            # missing_kwds={'color': 'lightgrey'},
            # edgecolor='black',
        )
        ctx.add_basemap(ax, source=ctx.providers.CartoDB.Voyager)
        fig.savefig(os.path.join(output_dir, "Non_residential_floor_area_per_ha.png"))
    # add data based on small area