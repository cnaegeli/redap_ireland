import os
import numpy as np
import logging

import helper

logger = logging.getLogger('data_cleaning.residential')


list_of_counties = ['Dublin City', 'Dún Laoghaire-Rathdown', 'Fingal', 'South Dublin', 'DUBLIN']

def convert_admin_areas(filename, epsg):
    admin_areas = helper.load_geo_data(
        filename,
        epsg=epsg
    )
    admin_areas= admin_areas[admin_areas['COUNTY'].isin(list_of_counties)]
    admin_areas.reset_index(inplace=True)
    useful_columns = {
        'CC_ID': 'area_id',
        'ENGLISH': 'name',
        'COUNTY': 'county_name',
    }
    admin_areas = admin_areas[list(useful_columns.keys()) + ['geometry']]
    admin_areas.rename(columns=useful_columns, inplace=True)
    admin_areas['area_id'] = admin_areas['area_id'].astype(int)
    admin_areas.set_index('area_id', inplace=True)
    admin_areas.sort_index(inplace=True)
    return admin_areas
        
def convert_census_electoral_divisions(filename_census_electoral_divisions, filname_dublin_eds, epsg):
    census_electoral_divisions = helper.load_geo_data(
        filename_census_electoral_divisions,
        epsg=epsg
    )
    census_electoral_divisions= census_electoral_divisions[census_electoral_divisions['COUNTY'].isin(list_of_counties)]
    census_electoral_divisions.reset_index(inplace=True)
    eds = helper.load_geo_data(
        filname_dublin_eds,
        epsg=epsg
    )

    rename_eds ={
        'raheny-st. assam': "RAHENY-St. ASSAM",
        'st. kevins': "SAINT KEVIN'S",
        'lucan-st. helens': "LUCAN-St. HELENS",
        'rathfarnham-st. endas': "RATHFARNHAM-St. ENDA'S",
        'terenure-st. james': "TERENURE-St. JAMES",
        'kilsallaghan': "KILSALLAGHAN",
        'rush': "RUSH",
        'dun laoghaire-sallynoggin east': "DUN LAOGHAIRE SALLYNOGGIN EAST",
        'dun laoghaire-sallynoggin south': "DUN LAOGHAIRE SALLYNOGGIN SOUTH",
        'dun laoghaire-sallynoggin west': "DUN LAOGHAIRE-SALLYNOGGIN WEST",
        }
    eds['eds'] = np.where(
        eds['eds'].isin(rename_eds.keys()),
        eds['eds'].map(rename_eds),
        eds['eds']
        )
    
    eds['eds'] = eds['eds'].str.upper()
    eds['postcodes'] = eds['postcodes'].str.title()
    census_electoral_divisions['ED_ENGLISH'] = census_electoral_divisions['ED_ENGLISH'].str.upper()
    census_electoral_divisions = census_electoral_divisions.merge(
        eds[['eds', 'postcodes']], left_on = 'ED_ENGLISH', right_on='eds', how='left'
        )
    
    useful_columns = {
        'CSOED_3409': 'area_id',
        'CSOED_34_1': 'name',
        'postcodes': 'contained_in',
        'COUNTY': 'county_name',
        'GUID_': 'GUID',
        'ED_ID': 'EDID',
        'OSIED_3441': 'OSIED',
    }
    census_electoral_divisions = census_electoral_divisions[list(useful_columns.keys()) + ['geometry']]
    census_electoral_divisions.rename(columns=useful_columns, inplace=True)
    census_electoral_divisions['area_id'] = census_electoral_divisions['area_id'].astype(int)
    census_electoral_divisions.set_index('area_id', inplace=True)
    census_electoral_divisions.sort_index(inplace=True)
    
    census_electoral_divisions['contained_in'] = np.where(
        census_electoral_divisions['contained_in'].isna(),
        census_electoral_divisions['name'].map({
            'Rush': 'Co. Dublin',
            'Kilsallaghan': 'Co. Dublin',
            }),
        census_electoral_divisions['contained_in']
        )
    return census_electoral_divisions


def convert_postcodes(filename, epsg):
    postcodes = helper.load_geo_data(
        filename,
        epsg=epsg
    )
    postcodes['postcodes'] = postcodes['postcodes'].str.title()

    
    useful_columns = {
        'postcodes': 'area_id',
    }
    postcodes = postcodes[list(useful_columns.keys()) + ['geometry']]
    postcodes.rename(columns=useful_columns, inplace=True)
    postcodes.set_index('area_id', inplace=True)
    postcodes.sort_index(inplace=True)
        
    return postcodes


def convert_small_areas(filename, epsg):
    small_areas = helper.load_geo_data(
        filename,
        epsg=epsg
        )
    small_areas= small_areas[small_areas['COUNTYNAME'].isin(list_of_counties)]
    small_areas.reset_index(inplace=True)

    useful_columns = {
        'GUID': 'area_id',
        'SA_PUB2011': 'name',
        'CSOED': 'contained_in',
        'COUNTYNAME': 'county_name',
        'OSIED': 'OSIED',
    }
    small_areas = small_areas[list(useful_columns.keys()) + ['geometry']]
    small_areas.rename(columns=useful_columns, inplace=True)
    small_areas['contained_in'] = small_areas['contained_in'].astype(int)
    small_areas.set_index('area_id', inplace=True)
    small_areas.sort_index(inplace=True)
    
    return small_areas


def convert_all_geodata(filenames, output_dir, epsg):

    admin_areas = convert_admin_areas(filenames['admin_areas'], epsg)
    census_electoral_divisions = convert_census_electoral_divisions(
        filenames['census_electoral_divisions'],
        filenames['dublin_eds'],
        epsg
    )
    postcodes = convert_postcodes(filenames['postcodes'], epsg)
    postcodes = census_electoral_divisions.dissolve(by='contained_in')
    postcodes = postcodes[['county_name', 'geometry']]
    postcodes['name'] = postcodes.index

    small_areas = convert_small_areas(filenames['small_areas'], epsg)
    map_postcodes = census_electoral_divisions['contained_in']
    map_postcodes.name = 'postcodes'

    small_areas = small_areas.merge(map_postcodes, left_on='contained_in', right_index=True, how='left')
    small_areas['postcodes'] = np.where(
            small_areas['postcodes'].isna(),
            'Dublin 8',
            small_areas['postcodes']
            )
    # census_electoral_divisions = small_areas.dissolve(by='contained_in')
    postcodes = small_areas.dissolve(by='postcodes')
    postcodes = postcodes[['county_name', 'geometry']]
    postcodes['name'] = postcodes.index
    # census_electoral_divisions = small_areas.dissolve(by='postcodes')


    # fig, ax = plt.subplots(1, figsize=(12, 12))
    # small_areas.boundary.plot(
    #     ax=ax,
    #     color='black',
    # )
    # census_electoral_divisions.boundary.plot(
    #     ax=ax,
    #     color='red',
    #     linestyle='--'
    # )

    admin_areas.to_file(
        os.path.join(output_dir, "Dublin_admin_areas.geojson"),
        driver='GeoJSON',
    )
    census_electoral_divisions.to_file(
        os.path.join(output_dir, "Dublin_census_electoral_divisions.geojson"),
        driver='GeoJSON',
    )
    postcodes.to_file(
        os.path.join(output_dir, "Dublin_postcodes.geojson"),
        driver='GeoJSON',
    )
    small_areas.to_file(
        os.path.join(output_dir, "Dublin_small_areas.geojson"),
        driver='GeoJSON',
    )