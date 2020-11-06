import geopandas as gpd

def load_geo_data(filename, epsg=None):
    data = gpd.read_file(filename)
    if epsg is not None:
        data.to_crs(epsg=epsg, inplace=True)
    return data

    