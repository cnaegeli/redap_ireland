import os
import time
import logging

import convert_geo_data
import residential
import non_residential

DEBUG = False

# control
CONVERT_GEODATA = False
GENERATE_RESIDENTIAL_STOCK = False
GENERATE_NON_RESIDENTIAL_STOCK = True
EPSG = 2157
LOAD_CLEANED_DATA_FILES = False

start_time = time.time()
intermediate_time = time.time()
timestamp = time.strftime("%Y_%m_%d__%H_%M_%S", time.localtime())

# define filenames
base_dir = os.path.abspath(os.path.join(__file__, "../../.."))

dir_data = os.path.join(
    base_dir,
    "data",
    "Ireland",
)
dir_geodata = os.path.join(
    dir_data,
    "Shape files",
)
dir_residenital_data = os.path.join(
    dir_data,
    "Residential",
)
dir_non_residential_data = os.path.join(
    dir_data,
    "Commercial",
)

dir_output = os.path.join(
    dir_data,
    "Output",
)
if not os.path.exists(dir_output):
    os.mkdir(dir_output)
dir_output_geodata = os.path.join(
    dir_output,
    "Geodata",
)
if not os.path.exists(dir_output_geodata):
    os.mkdir(dir_output_geodata)
dir_output_building_data = os.path.join(
    dir_output,
    "Building data",
)
if not os.path.exists(dir_output_building_data):
    os.mkdir(dir_output_building_data)

dir_logs = os.path.join(
    dir_output,
    ".log",
)
if not os.path.exists(dir_logs):
    os.mkdir(dir_logs)

#set filenames
filenames_orig_geodata = {
    'small_areas': os.path.join(dir_geodata, "Small_Areas_Ungeneralised__OSi_National_Statistical_Boundaries__2015.shp"),
    'postcodes': os.path.join(dir_geodata, "dublin_postcodes.shp"),
    'census_electoral_divisions': os.path.join(dir_geodata, "CSO_Electoral_Divisions_Generalised_100m__OSi_National_Statistical_Boundaries__2015.shp"),
    'dublin_eds': os.path.join(dir_geodata, "dublin_eds.shp"),
    'admin_areas': os.path.join(dir_geodata, "Administrative_Areas_OSi_National_Statutory_Boundaries_Ungeneralised.shp"),
}

filenames_residential = {
    'small_area_data': os.path.join(dir_residenital_data, "SAPS2016_SA2017.csv"),
    'small_area_data_cleaned': os.path.join(dir_output, "DublinSmallAreaData.csv"),
    'ber_data': os.path.join(dir_residenital_data, "BER", "Dublin BERs.xlsx"),
    'ber_data_cleaned': os.path.join(dir_output, "DublinBERData.csv"),
    'small_area_geodata': os.path.join(dir_output_geodata, "Dublin_small_areas.geojson"),
}

filenames_non_residential = {
    'orig_files': {
        "DDC": os.path.join(dir_non_residential_data, "DCC VO.csv"),
        "DLR": os.path.join(dir_non_residential_data, "DLR VO.csv"),
        "FCC": os.path.join(dir_non_residential_data, "FCC VO.csv"),
        "SDCC": os.path.join(dir_non_residential_data, "SDCC VO.csv"),
    },
    'non_res_use_categories': os.path.join(dir_non_residential_data, "map_non_res_uses.csv"),
    'non_res_data_cleaned': os.path.join(dir_output, "DublinNonResData.csv"),
    'small_area_data_cleaned': os.path.join(dir_output, "DublinSmallAreaData.csv"),
    'small_area_geodata': os.path.join(dir_output_geodata, "Dublin_small_areas.geojson"),
}

# set up logging
logger = logging.getLogger('data_cleaning')
logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)
formatter = logging.Formatter('%(asctime)s: %(name)s: %(message)s')

logger_file_handler = logging.FileHandler(os.path.join(dir_logs, timestamp+'.log'))
logger_file_handler.setLevel(logging.DEBUG if DEBUG else logging.INFO)
logger_file_handler.setFormatter(formatter)

logger_console_handler = logging.StreamHandler()
logger_console_handler.setLevel(logging.DEBUG if DEBUG else logging.INFO)
logger_console_handler.setFormatter(formatter)

logger.addHandler(logger_file_handler)
logger.addHandler(logger_console_handler)


if CONVERT_GEODATA:
    logger.info('Covnert geo data')
    convert_geo_data.convert_all_geodata(
        filenames_orig_geodata,
        dir_output_geodata, 
        EPSG
        )
    logger.info(f"Converted geodata in "
                f"{int((time.time() - intermediate_time)/60)} min")
    intermediate_time = time.time()
else:
    logger.info('Skipped step: Covnert geo data')

if GENERATE_RESIDENTIAL_STOCK:
    logger.info('Generate residential stock')
    residential.generate_stock(
        filenames_residential,
        dir_output_building_data, 
        LOAD_CLEANED_DATA_FILES, 
        generate_example_figures=True
        )
    logger.info(f"Residential stock generated in "
                f"{int((time.time() - intermediate_time)/60)} min")
    intermediate_time = time.time()
else:
    logger.info('Skipped step: Generate residential stock')

if GENERATE_NON_RESIDENTIAL_STOCK:
    logger.info('Generate non-residential stock')
    non_residential.generate_stock(
        filenames_non_residential,
        dir_output_building_data, 
        LOAD_CLEANED_DATA_FILES,
        generate_example_figures=True
        )
    logger.info(f"Non-residential stock generated in "
                f"{int((time.time() - intermediate_time) / 60)} min")
    intermediate_time = time.time()
else:
    logger.info('Skipped step: Generate res-residential stock')


logger.info(f"Finished in {int(time.time() - start_time)} seconds")

handlers = logger.handlers[:]
for handler in handlers:
    handler.close()
    logger.removeHandler(handler)
