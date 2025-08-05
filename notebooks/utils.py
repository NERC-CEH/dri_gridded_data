#
# Matt Brown, Dec 2019
#-----------------------------------------
#
# Utilities for processing abstraction data

import numpy as np
import xarray as xr
from shapely.geometry import Polygon, MultiPolygon, Point, MultiPoint
import geopandas as gpd
from rasterio import features
from affine import Affine
import s3fs


def get_coords(filein):
    filen = open(filein,'r')
    ncols = int(filen.readline().split()[1])
    nrows = int(filen.readline().split()[1])
    xllc  = float(filen.readline().split()[1])
    yllc  = float(filen.readline().split()[1])
    res   = float(filen.readline().split()[1])
    nodata= float(filen.readline().split()[1])
    filen.close()
    return nrows, ncols, xllc, yllc, res, nodata

def make_xarray(filein):
    nrows, ncols, xllc, yllc, res, nodata = get_coords(filein)
    xcoords = np.linspace(xllc + (res/2), xllc + (res/2) + (res*(ncols-1)), ncols)
    ycoords = np.linspace(yllc + (res/2), yllc + (res/2) +  (res*(nrows-1)), nrows)[::-1]
    vals = np.loadtxt(filein, skiprows=6)
    vals = xr.DataArray(vals, coords=[('y', ycoords), ('x', xcoords)])
    vals = vals.where(vals != nodata)
    return vals, xcoords, ycoords, nodata

def grd_to_netcdf_single(filein, varname, units, filesave=1, filenameout='grd.ncf', template=None):
    vals, junk, junk2, nodata = make_xarray(filein)
    vals.name = varname
    vals.attrs = {'Units': units, 'missing_value': nodata}
    vals.coords['x'].attrs = {'long_name':'easting', 'standard_name':'projection_x_coordinate', 'units':'m', 'point_spacing':'even', 'axis':'x'}
    vals.coords['y'].attrs = {'long_name':'northing', 'standard_name':'projection_y_coordinate', 'units':'m', 'point_spacing':'even', 'axis':'y'}

    if filesave == 1:
        vals.to_netcdf(filenameout)
    else:
        return vals, nodata


def catchment_subset_shapefile(data=None, datafile=None, multifile=0, sfname=None, endpoint=None, xname='x', yname='y', IDname=None, IDs=None, drop=0):
    '''
    Function to subset an xarray dataarray or dataset, or netcdf dataset, to selected shapes from 
    a shapefile. Returns an xarray dataset with of the same shape as the input
    datafile but with the data outside the selected shapes
    set to nans. Also returns the shapes so these can be plotted.

    data:     An xarray DataArray or DataSet
    datafile: The filename of the netcdf file to subset. Multiple files can be selected with * etc.
              If this is the case multifile should be set to 1. Defaults to 0.
    sfname:   The filepath of the shapefile
    endpoint: Used if sfname is an s3 path. The endpoint_url for the s3 storage system being used. 
    IDname:   The name of the catgeory to search over for selecting shapes to subset to (e.g. 'RIVER')
    IDs:      The values of the category to select (e.g. ['Thames', 'Severn'])
    multifile:Are multiple files specfied in datafile? Set to 1 if so. In this case the files are read in
              using dask, which can process data that exceeds the memory capacity of the machine by
              processing in parallel.
    xname: Name of the x-coordinate in the netcdf file(s). 'x' by default.
    yname: Name of the y-coordinate in the netcdf file(s). 'y' by default
    '''

    # Read in data
    if datafile:
        print('Reading in ' + datafile)
        if multifile == 1:
            data = xr.open_mfdataset(datafile, parallel=True)
        else:
            data = xr.load_dataset(filein)

    subset = add_shape_coord_from_data_array(data, sfname, endpoint, IDname, IDs, yname, xname)
    if drop == 0:
        subset = subset.where(subset[IDname]==1, other=np.nan)
    else:
        subset = subset.where(subset[IDname]==1, drop=True)
        
        
    return subset



def add_shape_coord_from_data_array(xr_da, shp_path, endpoint, IDname, IDs, latname, lonname):
    """ Create a new coord for the xr_da indicating whether or not it 
    is inside the shapefile
    
    Creates a new coord - "coord_name" which will have integer values
    used to subset xr_da for plotting / analysis/
    
    Usage:
    -----
    precip_da = add_shape_coord_from_data_array(precip_da, "awash.shp", "awash")
    awash_da = precip_da.where(precip_da.awash==0, other=np.nan) 
    """
    # 1. read in shapefile
    if shp_path[:5] == "s3://":
        s3 = s3fs.S3FileSystem(anon=True, endpoint_url=endpoint)
        shp_gpd = gpd.read_file(s3.open(shp_path))
    else:
        shp_gpd = gpd.read_file(shp_path)
    
    # 2. create a list of tuples (shapely.geometry, id)
    #    this allows for many different polygons within a .shp file (e.g. States of US)
    shapes = []
    counter = 0
    for ID in shp_gpd[IDname]:
        if ID in IDs:
            shapes.append((shp_gpd['geometry'][counter], 1))
            print('Found: ' + str(shp_gpd[IDname][counter]))
        counter+=1

    if len(shapes) == 0:
        raise AttributeError(IDname + ' ' + str(IDs) + ' not found in shapefile')

    xr_da[IDname] = rasterize(shapes, xr_da.coords,
                              longitude=lonname, latitude=latname)
    
        
    return xr_da

def transform_from_latlon(lat, lon):
    """ 
    input 1D array of lat / lon and output an Affine transformation
    """

    lat = np.asarray(lat)
    lon = np.asarray(lon)
    trans = Affine.translation(lon[0], lat[0])
    scale = Affine.scale(lon[1] - lon[0], lat[1] - lat[0])
    return trans * scale

def rasterize(shapes, coords, latitude='y', longitude='x',
              fill=np.nan, **kwargs):
    """
    Rasterize a list of (geometry, fill_value) tuples onto the given
    xarray coordinates. This only works for 1d latitude and longitude
    arrays.

    usage:
    -----
    1. read shapefile to geopandas.GeoDataFrame
          `states = gpd.read_file(shp_dir+shp_file)`
    2. encode the different shapefiles that capture those lat-lons as different
        numbers i.e. 0.0, 1.0 ... and otherwise np.nan
          `shapes = (zip(states.geometry, range(len(states))))`
    3. Assign this to a new coord in your original xarray.DataArray
          `ds['states'] = rasterize(shapes, ds.coords, longitude='X', latitude='Y')`

    arguments:
    ---------
    : **kwargs (dict): passed to `rasterio.rasterize` function

    attrs:
    -----
    :transform (affine.Affine): how to translate from latlon to ...?
    :raster (numpy.ndarray): use rasterio.features.rasterize fill the values
      outside the .shp file with np.nan
    :spatial_coords (dict): dictionary of {"X":xr.DataArray, "Y":xr.DataArray()}
      with "X", "Y" as keys, and xr.DataArray as values

    returns:
    -------
    :(xr.DataArray): DataArray with `values` of nan for points outside shapefile
      and coords `Y` = latitude, 'X' = longitude.
    """
                  
    print('Adding mask to xarray')
    transform = transform_from_latlon(coords[latitude], coords[longitude])
    out_shape = (len(coords[latitude]), len(coords[longitude]))
    raster = features.rasterize(shapes, out_shape=out_shape,
                                fill=fill, transform=transform,
                                dtype=float, **kwargs)
    spatial_coords = {latitude: coords[latitude], longitude: coords[longitude]}
    return xr.DataArray(raster, coords=spatial_coords, dims=(latitude, longitude))