"""
process:

census data:
- read in census datadata
- ipf to remove "<" signs from census data
- generate marignals for ipu routine
-

BER data:
- read in data:
- clean data (remove incomplete datasets)
- match year and type to census types
-
"""

# TODO: see how many dwellings there are that could be in the same building
# TODO: aggregate dwelligns to buildings
# TODO: generate marginals for buildings
# TODO: ipu for each postcode individually
# TODO: filter and rename
import os
import pandas as pd
import numpy as np
import synthpop.synthpop.zone_synthesizer as zs
import matplotlib.pyplot as plt
import contextily as ctx
import logging

import helper

logger = logging.getLogger('data_cleaning.residential')

def load_and_clean_small_area_data(filename):
    
    data = pd.read_csv(filename, delimiter=",", header=[0], index_col=[0], encoding="ISO-8859-1")
    
    remap_demensions = {
        'building_period': {
            'T6_2_PRE19H': 'Pre 1919',
            'T6_2_19_45H': '1919 - 1945',
            'T6_2_46_60H': '1946 - 1960',
            'T6_2_61_70H': '1961 - 1970',
            'T6_2_71_80H': '1971 - 1980',
            'T6_2_81_90H': '1981 - 1990',
            'T6_2_91_00H': '1991 - 2000',
            'T6_2_01_10H': '2001 - 2010',
            'T6_2_11LH': '2011 or Later',
            #'T6_2_NSH': 'Not stated (No. of households)
            },
        'building_type': {
            'T6_1_HB_H': 'Single-dwelling building',
            'T6_1_FA_H': 'Multi-dwelling building',
            'T6_1_BS_H': 'Multi-dwelling building',
            'T6_1_CM_H': 'Single-dwelling building',
            #'T6_1_NS_H': 'Not stated (No. of households)
            },
        'energy_carrier_space_heating': {
            'T6_5_NCH': 'No central heating',
            'T6_5_OCH': 'Oil',
            'T6_5_NGCH': 'Natural gas',
            'T6_5_ECH': 'Electricity',
            'T6_5_CCH': 'Coal',
            'T6_5_PCH': 'Peat',
            'T6_5_LPGCH': 'LPG',
            'T6_5_WCH': 'Wood',
            'T6_5_OTH': 'Other',
            #'T6_5_NS': 'Not stated
            },
        }


    dimensions = {
        'building_period': [
            'T6_2_PRE19H',
            'T6_2_19_45H',
            'T6_2_46_60H',
            'T6_2_61_70H',
            'T6_2_71_80H',
            'T6_2_81_90H',
            'T6_2_91_00H',
            'T6_2_01_10H',
            'T6_2_11LH',
            'T6_2_NSH',
            ],
        'building_type': [
            'T6_1_HB_H',
            'T6_1_FA_H',
            'T6_1_BS_H',
            'T6_1_CM_H',
            'T6_1_NS_H',
            ],
        'energy_carrier_space_heating': [
            'T6_5_NCH',
            'T6_5_OCH',
            'T6_5_NGCH',
            'T6_5_ECH',
            'T6_5_CCH',
            'T6_5_PCH',
            'T6_5_LPGCH',
            'T6_5_WCH',
            'T6_5_OTH',
            'T6_5_NS',
            ],
        }

    output = []
    for i, (dimension, columns) in enumerate(dimensions.items()):
        subset = data[columns].T
        subset = subset.reset_index(drop=False)
        subset['index'] = subset['index'].map(remap_demensions[dimension])
        subset = subset.rename(columns={'index':dimension})
        total = subset.sum()
        subset['category'] = dimension
        subset = subset.groupby(['category', dimension]).sum()
        subset = subset / subset.sum() * total
        # turn all into integers
        # for column in subset.columns:
        #     column_total = subset[column].sum()
        #     column_values = subset[column].values
        #     if not all(column_values.astype(int) == column_values):
        #         r = hl.integerise(subset[column].values.astype(float))
        #         subset[column] = r['result']
        #     else:
        #         subset[column] = column_values.astype(int)
        subset = subset.T
        output.append(subset)
    
    output = pd.concat(output, axis = 1)
    
    output.columns = output.columns.set_names(['cat_name', 'cat_values'])
    output.index.name = 'zone_id'
    return output


def load_and_clean_ber_data(filename, max_year = 2016):
    
    #total 270439
    data = pd.read_excel(filename, "Dublin Data", header = [0])
    
    #data.dropna(subset=['MainSpaceHeatingFuel'], inplace=True)
    
    # data.drop_duplicates(inplace=True)
    
    data.drop(data[
        (data.UValueWindow == 0)
        ].index, inplace=True)
    data.drop(data[
        (data.UValueWall == 0)
        ].index, inplace=True)
    data.drop(data[
        (data['MainSpaceHeatingFuel'].isna())
        ].index, inplace=True)
    data.drop(data[
        (data['MainWaterHeatingFuel'].isna())
        ].index, inplace=True)
    data.drop(data[
        (data['VentilationMethod'].isna())
        ].index, inplace=True)


    useful_columns = [
        'CountyName',
        'DwellingTypeDescr',
        'Year_of_Construction',
        'TypeofRating',
        'GroundFloorArea(sq m)',
        'UValueWall',
        'UValueRoof',
        'UValueFloor',
        'UValueWindow',
        'UvalueDoor',
        'WallArea',
        'RoofArea',
        'FloorArea',
        'WindowArea',
        'DoorArea',
        'NoStoreys',
        'MainSpaceHeatingFuel',
        'MainWaterHeatingFuel',
        'HSMainSystemEfficiency',
        'MultiDwellingMPRN',
        'HSEffAdjFactor',
        'HSSupplHeatFraction',
        'HSSupplSystemEff',
        'WHMainSystemEff',
        'WHEffAdjFactor',
        'SupplSHFuel',
        'SupplWHFuel',
        'SHRenewableResources',
        'WHRenewableResources',
        'VentilationMethod',
        'FanPowerManuDeclaredValue',
        'HeatExchangerEff',
        'StructureType',
        'NoOfSidesSheltered',
        'WarmAirHeatingSystem',
        'GroundFloorUValue',
        'SolarHotWaterHeating',
        'ApertureArea',
        'SolarHeatFraction',
        'GroundFloorArea',
        'GroundFloorHeight',
        'FirstFloorArea',
        'FirstFloorHeight',
        'SecondFloorArea',
        'SecondFloorHeight',
        'ThirdFloorArea',
        'ThirdFloorHeight',
        'ThermalBridgingFactor',
        'ThermalMassCategory',
        'LivingAreaPercent',
        'HESSchemeUpgrade',
        'RoomInRoofArea',
        'PurposeOfRating',
        'FirstBoilerFuelType',
        'FirstHeatGenPlantEff',
        'FirstPercentageHeat',
        'SecondBoilerFuelType',
        'SecondHeatGenPlantEff',
        'SecondPercentageHeat',
        'ThirdBoilerFuelType',
        'ThirdHeatGenPlantEff',
        'ThirdPercentageHeat',
        'SolarSpaceHeatingSystem',
         'DeliveredLightingEnergy',
         'DeliveredEnergyPumpsFans',
         'DeliveredEnergyMainWater',
         'DeliveredEnergyMainSpace',
         'PrimaryEnergyLighting',
         'PrimaryEnergyPumpsFans',
         'PrimaryEnergyMainWater',
         'PrimaryEnergyMainSpace',
         'CO2Lighting',
         'CO2PumpsFans',
         'CO2MainWater',
         'CO2MainSpace',
        ]
    
    data = data[useful_columns]
    data['dwelling_type'] = data['DwellingTypeDescr'].map({
        'Apartment': 'All Apartments and Bed-sits',
        'Basement Dwelling': 'All Apartments and Bed-sits',
        'Detached house': 'Detached house',
        'End of terrace house': 'Terraced house',
        'Ground-floor apartment': 'All Apartments and Bed-sits',
        'House': 'Detached house',
        'Maisonette': 'All Apartments and Bed-sits',
        'Mid-floor apartment': 'All Apartments and Bed-sits',
        'Mid-terrace house': 'Terraced house',
        'Semi-detached house': 'Semi-detached house',
        'Top-floor apartment': 'All Apartments and Bed-sits',
        })
    data['building_type'] = data['dwelling_type'].map({
        'All Apartments and Bed-sits': 'Multi-dwelling building',
        'Detached house': 'Single-dwelling building',
        'Terraced house': 'Single-dwelling building',
        'Semi-detached house': 'Single-dwelling building',
        })
    def get_building_period(year, max_year=max_year):
        if year < 1919:
            bp = 'Pre 1919'
        elif year < 1946:
            bp = '1919 - 1945'
        elif year < 1961:
            bp = '1946 - 1960'
        elif year < 1971:
            bp = '1961 - 1970'
        elif year < 1981:
            bp = '1971 - 1980'
        elif year < 1991:
            bp = '1981 - 1990'
        elif year < 2001:
            bp = '1991 - 2000'
        elif year < 2011:
            bp = '2001 - 2010'
        elif year < max_year:
            bp = '2011 or Later'
        else:
            bp = None
        return bp
    data['building_period'] = data['Year_of_Construction'].map(get_building_period)
    data = data[data['Year_of_Construction']<max_year]
    data.reset_index(inplace=True)
    data['MainSpaceHeatingFuel'] = data['MainSpaceHeatingFuel'].str.strip()
    data['MainWaterHeatingFuel'] = data['MainWaterHeatingFuel'].str.strip()
    map_energy_carrier = {
        'Anthracite': 'Coal',
        'Biodiesel from renewable sourc': 'Other',
        'Bioethanol from renewable sour': 'Other',
        'Bottled LPG': 'LPG',
        'Bulk LPG (propane or butane)': 'LPG',
        'Electricity': 'Electricity',
        'Electricity - Off-peak Night-R': 'Electricity',
        'Heating Oil': 'Oil',
        'House Coal': 'Coal',
        'Mains Gas': 'Natural gas',
        'Manufactured Smokeless Fuel': 'Coal',
        'Peat Briquettes': 'Peat',
        'Solid Multi-Fuel': 'Coal',
        'Wood Chips': 'Wood',
        'Wood Logs': 'Wood',
        'Wood Pellets (bulk supply for': 'Wood',
        'Wood Pellets (in bags for seco': 'Wood',
        }
    data['energy_carrier_space_heating'] = data['MainSpaceHeatingFuel'].map(
        map_energy_carrier
        )
    data['energy_carrier_space_heating'] = np.where(
        data['energy_carrier_space_heating'].isin(map_energy_carrier.values()),
        data['energy_carrier_space_heating'],
        'No central heating',
        )
    data = data[data['building_period'].isna()==False]
    data = data[data['building_type'].isna()==False]
    data = data[data['energy_carrier_space_heating'].isna()==False]
    return data


def generate_stock(
    filenames,
    output_dir,
    load_cleaned_data=False,
    generate_example_figures=False
):
    
    if load_cleaned_data:
        logger.info('Load cleaned data')
        small_area_data = pd.read_csv(filenames['small_area_data_cleaned'], delimiter=';', header=[0,1], index_col=[0])
        ber_data = pd.read_csv(filenames['ber_data_cleaned'], delimiter=';', header=0)
    else:
        logger.info('Load and clean data')
        small_area_data = load_and_clean_small_area_data(filenames['small_area_data'])
        small_area_data.to_csv(filenames['small_area_data_cleaned'], sep=';', header=True)
        ber_data = load_and_clean_ber_data(filenames['ber_data'])
        ber_data.to_csv(filenames['ber_data_cleaned'], sep=';', header=True)

    small_areas = helper.load_geo_data(filenames['small_area_geodata'])

    logger.info('Prepare input data to ipu')
    map_sample_geog = small_areas.groupby('postcodes')['area_id'].first().to_dict()

    ber_data = ber_data.sort_values(by=['CountyName'])
    ber_data.reset_index(inplace=True, drop=True)
    ber_data['sample_geog'] = ber_data['CountyName'].map(
        map_sample_geog
        )
    #ber_data['sample_geog'] = '4c07d11d-fd01-851d-e053-ca3ca8c0ca7f'
    ber_data['serialno'] = np.arange(100000000, 100000000 + len(ber_data))
    ber_data.reset_index(inplace=True, drop=True)

    # seperate ber data into building and dwleling data
    sample_building_stock = ber_data[['building_period', 'building_type', 'sample_geog', 'serialno']].copy(deep=True)
    sample_dwelling_stock = ber_data.copy(deep=True)
    #del sample_dwelling_stock['building_period']
    #del sample_dwelling_stock['building_type']

    # generate marginals
    small_area_data_dublin = small_area_data[small_area_data.index.isin(small_areas['area_id'])]
    marginals_buildings = small_area_data_dublin[['building_type', 'building_period']].copy(deep=True)
    marginals_dwellings = small_area_data_dublin[['energy_carrier_space_heating']].copy(deep=True)

    # generate crosswalk list
    crosswalk = small_areas.apply(
        lambda x: (x['area_id'], map_sample_geog[x['postcodes']]), axis = 1
        ).tolist()

    # crosswalk = small_areas.apply(
    #     lambda x: (x['area_id'], '4c07d11d-fd01-851d-e053-ca3ca8c0ca7f'), axis = 1
    #     ).tolist()
    
    logger.info('Iterative proportional fitting')
    all_buildings = []
    all_dwellings = []
    all_stats = []
    for postcode, ref_small_area in map_sample_geog.items():

        sample_building_stock_subset = sample_building_stock[sample_building_stock['sample_geog']==ref_small_area]
        sample_building_stock_subset.reset_index(inplace=True, drop=True)
        sample_dwelling_stock_subset = sample_dwelling_stock[sample_dwelling_stock['sample_geog']==ref_small_area]
        sample_dwelling_stock_subset.reset_index(inplace=True, drop=True)

        relevant_areas = small_areas[small_areas['postcodes'].isin([postcode])]
        m_b = marginals_buildings[marginals_buildings.index.isin(small_areas['area_id'])]
        m_d = marginals_dwellings[marginals_dwellings.index.isin(small_areas['area_id'])]

        crosswalk = relevant_areas.apply(
            lambda x: (x['area_id'], ref_small_area), axis = 1
            ).tolist()


        buildings, dwellings, stats = zs.synthesize_all_zones(
            m_b,
            m_d,
            sample_building_stock_subset,
            sample_dwelling_stock_subset,
            crosswalk
            )
        all_buildings.append(buildings)
        all_dwellings.append(dwellings)
        all_stats.append(stats)

    logger.info('Write output data')
    all_buildings = pd.concat(all_buildings)
    all_dwellings = pd.concat(all_dwellings)
    all_stats = pd.concat(all_stats)

    all_buildings.rename(columns={
        'household_id': 'building_id',
        'geog': 'area_id',
        }, inplace=True)
    all_dwellings.rename(columns={
        'household_id': 'building_id',
        'geog': 'area_id',
        }, inplace=True)
    output = all_dwellings[all_dwellings['energy_carrier_space_heating']!='No central heating']
    #output = all_buildings[['building_id', 'building_period', 'building_type']].merge(all_dwellings, left_on='building_id', right_on='building_id', how='inner')

    output.to_csv(
        os.path.join(output_dir, 'Dublin_Dwellings.csv'), sep=';', header=True
    )
    all_stats.to_csv(
        os.path.join(output_dir, 'Dublin_stats.csv'), sep=';', header=True
    )
    if generate_example_figures:
        logger.info('Generate example maps')
        
        all_dwellings['TotalDeliveredEnergy'] = (
            all_dwellings['DeliveredLightingEnergy']
            + all_dwellings['DeliveredEnergyPumpsFans']
            + all_dwellings['DeliveredEnergyMainWater']
            + all_dwellings['DeliveredEnergyMainSpace']
        )
        
        all_dwellings['TotalDeliveredHeat'] = (
            all_dwellings['DeliveredEnergyMainWater']
            + all_dwellings['DeliveredEnergyMainSpace']
        )
        
        results = all_dwellings.groupby('area_id')['TotalDeliveredEnergy', 'TotalDeliveredHeat'].sum()
        results = results / 1000
        results = small_areas.merge(results, left_on='area_id', right_index=True, how='inner')
        results['TotalDeliveredEnergyPerHa'] = results['TotalDeliveredEnergy'] / results.geometry.area
        results['TotalDeliveredHeatPerHa'] = results['TotalDeliveredHeat'] / results.geometry.area
        results = results.to_crs(epsg=3857)
        
        fig, ax = plt.subplots(1, figsize=(12, 12))
        results[results['county_name']=='Dublin City'].plot(
            column='TotalDeliveredEnergy',
            ax=ax,
            cmap='YlOrRd',
            alpha = 0.75,
            legend=True,
            scheme="quantiles",
            classification_kwds={'k':5},
            #legend_kwds={'label': "Energy demand [MWh/year]"},
            # missing_kwds={'color': 'lightgrey'},
            # edgecolor='black',
        )
        ctx.add_basemap(ax, source=ctx.providers.CartoDB.Voyager)
        fig.savefig(os.path.join(output_dir,"Residential_energy_demand.png"))
        
        fig, ax = plt.subplots(1, figsize=(12, 12))
        results[results['county_name']=='Dublin City'].plot(
            column='TotalDeliveredHeat',
            ax=ax,
            cmap='YlOrRd',
            alpha = 0.75,
            legend=True,
            scheme="quantiles",
            classification_kwds={'k':5},
            #legend_kwds={'label': "Heat demand [MWh/year]"},
            # missing_kwds={'color': 'lightgrey'},
            # edgecolor='black',
        )
        ctx.add_basemap(ax, source=ctx.providers.CartoDB.Voyager)
        fig.savefig(os.path.join(output_dir,"Residential_heat_demand.png"))
        
        fig, ax = plt.subplots(1, figsize=(12, 12))
        results[results['county_name']=='Dublin City'].plot(
            column='TotalDeliveredEnergyPerHa',
            ax=ax,
            cmap='YlOrRd',
            alpha = 0.75,
            legend=True,
            scheme="quantiles",
            classification_kwds={'k':5},
            #legend_kwds={'label': "Energy demand density [MWh/ha year]"},
            # missing_kwds={'color': 'lightgrey'},
            # edgecolor='black',
        )
        ctx.add_basemap(ax, source=ctx.providers.CartoDB.Voyager)
        fig.savefig(os.path.join(output_dir,"Residential_energy_demand_density.png"))
        
        fig, ax = plt.subplots(1, figsize=(12, 12))
        results[results['county_name']=='Dublin City'].plot(
            column='TotalDeliveredHeatPerHa',
            ax=ax,
            cmap='YlOrRd',
            alpha = 0.75,
            legend=True,
            scheme="quantiles",
            classification_kwds={'k':5},
            #legend_kwds={'label': "Heat demand density [MWh/ha year]"},
            # missing_kwds={'color': 'lightgrey'},
            # edgecolor='black',
        )
        ctx.add_basemap(ax, source=ctx.providers.CartoDB.Voyager)
        fig.savefig(os.path.join(output_dir,"Residential_heat_demand_density.png"))
"""

all_buildings, all_dwellings, all_stats = zs.synthesize_all_zones(
    marginals_buildings, 
    marginals_dwellings, 
    sample_building_stock,
    sample_dwelling_stock, 
    crosswalk
    )

all_buildings.rename(columns={
    'household_id': 'building_id',
    'geog': 'area_id',
    }, inplace=True)
all_dwellings.rename(columns={
    'household_id': 'building_id',
    'geog': 'area_id',
    }, inplace=True)
all_dwellings[all_dwellings['energy_carrier_space_heating']!='No central heating']


output_dir =  os.path.join(
    base_dir,
    "data",
    "Ireland", 
    "output",
)
all_dwellings.to_csv(
    os.path.join(output_dir, 'Dublin_Dwellings.csv'), sep=';', header=True
)
all_stats.to_csv(
    os.path.join(output_dir, 'Dublin_stats.csv'), sep=';', header=True
)

data = ber_data[ber_data['building_type']=='Multi-dwelling building']
a = data.groupby([
    'CountyName',
    'building_type',
    'Year_of_Construction',
    'TypeofRating',
    'UValueWall',
    'UValueWindow',
    'MainSpaceHeatingFuel',
    'HSMainSystemEfficiency',
    'SupplSHFuel',
    'HSSupplSystemEff',
    'MainWaterHeatingFuel',
    'WHMainSystemEff',
    'SupplWHFuel',
    'SolarHotWaterHeating',
    'VentilationMethod',
    'FanPowerManuDeclaredValue',
    'HeatExchangerEff',
    'StructureType',
    'ThermalMassCategory',
    ]).count()

filename_census =  os.path.join(
    base_dir,
    "data",
    "Ireland",
    "Residential",
    "Census 54954 Private Households in Perm House Units by Accom, Year Blt in Dublin_ EDs, 2016.xlsx"
)
census_data = load_and_clean_census_data(filename_census)



['CountyName',
 'DwellingTypeDescr',
 'Year_of_Construction',
 'TypeofRating',
 'EnergyRating',
 'BerRating',
 'GroundFloorArea(sq m)',
 'UValueWall',
 'UValueRoof',
 'UValueFloor',
 'UValueWindow',
 'UvalueDoor',
 'WallArea',
 'RoofArea',
 'FloorArea',
 'WindowArea',
 'DoorArea',
 'NoStoreys',
 'CO2Rating',
 'MainSpaceHeatingFuel',
 'MainWaterHeatingFuel',
 'HSMainSystemEfficiency',
 'MultiDwellingMPRN',
 'TGDLEdition',
 'MPCDERValue',
 'HSEffAdjFactor',
 'HSSupplHeatFraction',
 'HSSupplSystemEff',
 'WHMainSystemEff',
 'WHEffAdjFactor',
 'SupplSHFuel',
 'SupplWHFuel',
 'SHRenewableResources',
 'WHRenewableResources',
 'NoOfChimneys',
 'NoOfOpenFlues',
 'NoOfFansAndVents',
 'NoOfFluelessGasFires',
 'DraftLobby',
 'VentilationMethod',
 'FanPowerManuDeclaredValue',
 'HeatExchangerEff',
 'StructureType',
 'SuspendedWoodenFloor',
 'PercentageDraughtStripped',
 'NoOfSidesSheltered',
 'PermeabilityTest',
 'PermeabilityTestResult',
 'TempAdjustment',
 'HeatSystemControlCat',
 'HeatSystemResponseCat',
 'NoCentralHeatingPumps',
 'CHBoilerThermostatControlled',
 'NoOilBoilerHeatingPumps',
 'OBBoilerThermostatControlled',
 'OBPumpInsideDwelling',
 'NoGasBoilerHeatingPumps',
 'WarmAirHeatingSystem',
 'UndergroundHeating',
 'GroundFloorUValue',
 'DistributionLosses',
 'StorageLosses',
 'ManuLossFactorAvail',
 'SolarHotWaterHeating',
 'ElecImmersionInSummer',
 'CombiBoiler',
 'KeepHotFacility',
 'WaterStorageVolume',
 'DeclaredLossFactor',
 'TempFactorUnadj',
 'TempFactorMultiplier',
 'InsulationType',
 'InsulationThickness',
 'PrimaryCircuitLoss',
 'CombiBoilerAddLoss',
 'ElecConsumpKeepHot',
 'ApertureArea',
 'ZeroLossCollectorEff',
 'CollectorHeatLossCoEff',
 'AnnualSolarRadiation',
 'OvershadingFactor',
 'CylinderStat',
 'SolarStorageVolume',
 'VolumeOfPreHeatStore',
 'CombinedCylinder',
 'ElectricityConsumption',
 'SWHPumpSolarPowered',
 'ChargingBasisHeatConsumed',
 'gsdHSSupplHeatFraction',
 'gsdHSSupplSystemEff',
 'DistLossFactor',
 'CHPUnitHeatFraction',
 'CHPSystemType',
 'CHPElecEff',
 'CHPHeatEff',
 'CHPFuelType',
 'SupplHSFuelTypeID',
 'gsdSHRenewableResources',
 'gsdWHRenewableResources',
 'SolarHeatFraction',
 'DeliveredLightingEnergy',
 'DeliveredEnergyPumpsFans',
 'DeliveredEnergyMainWater',
 'DeliveredEnergyMainSpace',
 'PrimaryEnergyLighting',
 'PrimaryEnergyPumpsFans',
 'PrimaryEnergyMainWater',
 'PrimaryEnergyMainSpace',
 'CO2Lighting',
 'CO2PumpsFans',
 'CO2MainWater',
 'CO2MainSpace',
 'GroundFloorArea',
 'GroundFloorHeight',
 'FirstFloorArea',
 'FirstFloorHeight',
 'SecondFloorArea',
 'SecondFloorHeight',
 'ThirdFloorArea',
 'ThirdFloorHeight',
 'ThermalBridgingFactor',
 'ThermalMassCategory',
 'PredominantRoofTypeArea',
 'PredominantRoofType',
 'LowEnergyLightingPercent',
 'TotalDeliveredEnergy',
 'DeliveredEnergySecondarySpace',
 'DeliveredEnergySupplementaryWater',
 'LivingAreaPercent',
 'CO2SecondarySpace',
 'CO2SupplementaryWater',
 'PrimaryEnergySecondarySpace',
 'PrimaryEnergySupplementaryWater',
 'HESSchemeUpgrade',
 'RoomInRoofArea',
 'PurposeOfRating',
 'DateOfAssessment',
 'FirstEnergyTypeId',
 'FirstEnergyType_Description',
 'FirstEnerProdComment',
 'FirstEnerProdDelivered',
 'FirstPartLTotalContribution',
 'FirstEnerProdConvFactor',
 'FirstEnerProdCO2EmissionFactor',
 'FirstEnerConsumedComment',
 'FirstEnerConsumedDelivered',
 'FirstEnerConsumedConvFactor',
 'FirstEnerConsumedCO2EmissionFactor',
 'SecondEnergyTypeId',
 'SecondEnergyType_Description',
 'SecondEnerProdComment',
 'SecondEnerProdDelivered',
 'SecondPartLTotalContribution',
 'SecondEnerProdConvFactor',
 'SecondEnerProdCO2EmissionFactor',
 'SecondEnerConsumedComment',
 'SecondEnerConsumedDelivered',
 'SecondEnerConsumedConvFactor',
 'SecondEnerConsumedCO2EmissionFactor',
 'ThirdEnergyTypeId',
 'ThirdEnergyType_Description',
 'ThirdEnerProdComment',
 'ThirdEnerProdDelivered',
 'ThirdPartLTotalContribution',
 'ThirdEnerProdConvFactor',
 'ThirdEnerProdCO2EmissionFactor',
 'ThirdEnerConsumedComment',
 'ThirdEnerConsumedDelivered',
 'ThirdEnerConsumedConvFactor',
 'ThirdEnerConsumedCO2EmissionFactor',
 'FirstBoilerFuelType',
 'FirstHeatGenPlantEff',
 'FirstPercentageHeat',
 'SecondBoilerFuelType',
 'SecondHeatGenPlantEff',
 'SecondPercentageHeat',
 'ThirdBoilerFuelType',
 'ThirdHeatGenPlantEff',
 'ThirdPercentageHeat',
 'SolarSpaceHeatingSystem',
 'TotalPrimaryEnergyFact',
 'TotalCO2Emissions',
 'FirstWallType_Description',
 'FirstWallDescription',
 'FirstWallArea',
 'FirstWallUValue',
 'FirstWallIsSemiExposed',
 'FirstWallAgeBandId',
 'FirstWallTypeId',
 'SecondWallType_Description',
 'SecondWallDescription',
 'SecondWallArea',
 'SecondWallUValue',
 'SecondWallIsSemiExposed',
 'SecondWallAgeBandId',
 'SecondWallTypeId',
 'ThirdWallType_Description',
 'ThirdWallDescription',
 'ThirdWallArea',
 'ThirdWallUValue',
 'ThirdWallIsSemiExposed',
 'ThirdWallAgeBandId',
 'ThirdWallTypeId']

# output = result.drop(result[result['total'] == 0].index)

# writer = pd.ExcelWriter("BSM_CH_HeatingSystemStructure.xlsx")
# output.to_excel(writer, 'weights')
# writer.save()

"""