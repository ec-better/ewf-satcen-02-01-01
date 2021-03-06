import cioppy
import geopandas as gp
import os
import pandas as pd
from py_snap_helpers import *
from shapely.geometry import box
from snappy import jpy
from snappy import ProductIO
import gdal
import osr
import ogr
from shapely.geometry import box
import json
import sys

sys.path.append('/opt/OTB/lib/python')
sys.path.append('/opt/OTB/lib/libfftw3.so.3')
sys.path.append('/opt/anaconda/bin')
os.environ['OTB_APPLICATION_PATH'] = '/opt/OTB/lib/otb/applications'
os.environ['LD_LIBRARY_PATH'] = '/opt/OTB/lib'
os.environ['ITK_AUTOLOAD_PATH'] = '/opt/OTB/lib/otb/applications'

import otbApplication
from gdal_calc import Calc as gdalCalc

def get_metadata(input_references, data_path):
    
    ciop = cioppy.Cioppy()

    if isinstance(input_references, str):

        search_params = dict()

        search_params['do'] = 'terradue'

        products = gp.GeoDataFrame(ciop.search(end_point=input_references, 
                            params=search_params,
                            output_fields='identifier,self,wkt,startdate,enddate,enclosure,orbitDirection,track,orbitNumber', 
                            model='EOP'))

    else:    

        temp_results = []

        for index, self in enumerate(input_references):

            search_params = dict()

            search_params['do'] = 'terradue'

            temp_results.append(ciop.search(end_point=self, 
                                params=search_params,
                                output_fields='identifier,self,wkt,startdate,enddate,enclosure,orbitDirection,track,orbitNumber', 
                                model='EOP')[0])

        products = gp.GeoDataFrame(temp_results)
        
        products = products.merge(products.apply(lambda row: analyse(row, data_path), axis=1),
                                    left_index=True,
                                  right_index=True)
        
        
    return products

def analyse(row, data_path):
    
    series = dict()

    series['local_path'] = os.path.join(data_path, row['identifier'], row['identifier'] + '.SAFE', 'manifest.safe')
   
    return pd.Series(series)

def group_analysis(df):
    df['ordinal_type'] = 'NaN'
    slave_date=df['startdate'].min()[:10]
    master_date=df['startdate'].max()[:10]
    for i in range(len(df)):
    
        if slave_date == df.iloc[i]['startdate'][:10]:
            df.loc[i,'ordinal_type']='slave'
    
        elif master_date == df.iloc[i]['startdate'][:10]:
            df.loc[i,'ordinal_type']='master'

    return 

def bbox_to_wkt(bbox):
    
    return box(*[float(c) for c in bbox.split(',')]).wkt


def pre_process(products, aoi, utm_zone, resolution='10.0', polarization=None, orbit_type=None, show_graph=False):
    master_products=products[products['ordinal_type']=='master'].reset_index(drop=True)
    slave_products=products[products['ordinal_type']=='slave'].reset_index(drop=True)
    
####Read and Assemble Masters
    master_graph=GraphProcessor()
    master_read_nodes = []
    output_name_m='mst_' + master_products.iloc[0]['identifier'][:25]
    
    for index, product in master_products.iterrows():
        
        output_name_m += '_'+product['identifier'][-4:]
        operator = 'Read'
        parameters = get_operator_default_parameters(operator)
        node_id = 'Read-M-{0}'.format(index)
        source_node_id = ''
        parameters['file'] = product.local_path 
        master_graph.add_node(node_id,
                         operator, 
                         parameters,
                         source_node_id)
        source_node_id_m = node_id
        master_read_nodes.append(node_id)

    if len(master_read_nodes)>1:
        
        source_nodes_id = master_read_nodes
        operator = 'SliceAssembly'
        node_id = 'SliceAssembly-M'
        parameters = get_operator_default_parameters(operator)
        parameters['selectedPolarisations'] = polarization
        master_graph.add_node(node_id,
                         operator, 
                         parameters,
                         source_nodes_id)
        source_node_id_m = node_id
    
 ###### Read and Assemble Slaves    
    
    slave_read_nodes = []
    slave_graph = GraphProcessor()
    output_name_s = 'slv_'+ slave_products.iloc[0]['identifier'][:25]
    for index, product in slave_products.iterrows():
        output_name_s += '_'+product['identifier'][-4:]
        operator = 'Read'
        parameters = get_operator_default_parameters(operator)
        node_id = 'Read-S-{0}'.format(index)
        source_node_id = ''
        parameters['file'] = product.local_path 
        slave_graph.add_node(node_id,
                         operator, 
                         parameters,
                         source_node_id)
        source_node_id_s = node_id
        slave_read_nodes.append(node_id)
        
        
    if len(slave_read_nodes)>1:
        
        source_nodes_id = slave_read_nodes
        operator = 'SliceAssembly'
        node_id = 'SliceAssembly-S'
        parameters = get_operator_default_parameters(operator)
        parameters['selectedPolarisations'] = polarization
        slave_graph.add_node(node_id,
                         operator, 
                         parameters,
                         source_nodes_id)
        source_node_id_s = node_id
       
  ######Continue pre-processing master & slave products in two seperate graphs
    
    operator = 'Subset'   
    parameters = get_operator_default_parameters(operator)
    parameters['geoRegion'] = aoi
    parameters['copyMetadata'] = 'true'
    node_id = 'Subset-S'
    slave_graph.add_node(node_id,
                         operator,
                         parameters,
                         source_node_id_s)
    source_node_id_s = node_id
    node_id = 'Subset-M'
    master_graph.add_node(node_id,
                         operator,
                         parameters,
                         source_node_id_m)

    source_node_id_m = node_id

    
    operator = 'Apply-Orbit-File'
    parameters = get_operator_default_parameters(operator)
    if orbit_type == 'Restituted':
        parameters['orbitType'] = 'Sentinel Restituted (Auto Download)'
    node_id = 'Apply-Orbit-File-S'
    slave_graph.add_node(node_id,
                         operator,
                         parameters,
                         source_node_id_s)
    source_node_id_s = node_id
    
    node_id = 'Apply-Orbit-File-M'
    master_graph.add_node(node_id,
                         operator,
                         parameters,
                         source_node_id_m)

    source_node_id_m = node_id
 
    
    operator = 'Calibration'
    parameters = get_operator_default_parameters(operator)
    parameters['outputSigmaBand'] = 'true'
    if polarization is not None:
        parameters['selectedPolarisations'] = polarization
    node_id = 'Calibration-S'
    slave_graph.add_node(node_id,
                         operator,
                         parameters,
                         source_node_id_s)
    source_node_id_s = node_id
    
    node_id = 'Calibration-M'
    master_graph.add_node(node_id,
                         operator,
                         parameters,
                         source_node_id_m)

    source_node_id_m = node_id
    
    
    operator = 'Terrain-Correction'
    parameters = get_operator_default_parameters(operator)
    map_proj = utm_zone

    parameters['mapProjection'] = map_proj
    parameters['pixelSpacingInMeter'] = resolution            
    parameters['nodataValueAtSea'] = 'false'
    parameters['demName'] = 'SRTM 1Sec HGT'
    
    node_id = 'Terrain-Correction-S'
    slave_graph.add_node(node_id,
                         operator,
                         parameters,
                         source_node_id_s)
    source_node_id_s = node_id
    
    node_id = 'Terrain-Correction-M'
    master_graph.add_node(node_id,
                         operator,
                         parameters,
                         source_node_id_m)

    source_node_id_m = node_id
    
    operator = 'Write'
    parameters = get_operator_default_parameters(operator)
    parameters['formatName'] = 'BEAM-DIMAP' 
    
     
    node_id = 'Write-S'
    parameters['file'] = output_name_s
    slave_graph.add_node(node_id,
                         operator,
                         parameters,
                         source_node_id_s)
    
    
    node_id = 'Write-M'
    parameters['file'] =  output_name_m
    master_graph.add_node(node_id,
                         operator,
                         parameters,
                         source_node_id_m)

    if show_graph: 
            master_graph.view_graph()
            slave_graph.view_graph()
    
    
    master_graph.run()
    slave_graph.run()
    return [output_name_m,output_name_s]
        

def create_stack(products, show_graph=True):
    
    mygraph = GraphProcessor()
    
    operator = 'ProductSet-Reader'
    parameters = get_operator_default_parameters(operator)
    
    parameters['fileList'] = ','.join([ '{}.dim'.format(n) for n in products])
    
    node_id = 'ProductSet-Reader'
    source_node_id = ''
    
    #parameters['file'] = product.local_path 
    mygraph.add_node(node_id,
                     operator, 
                     parameters,
                     '')

    source_node_id = node_id
    
    operator = 'CreateStack'
    parameters = get_operator_default_parameters(operator)
    node_id = 'CreateStack'
        
    parameters['extent'] = 'Minimum'
    parameters['resamplingType'] = 'BICUBIC_INTERPOLATION'
    
    mygraph.add_node(node_id,
                     operator, 
                     parameters,
                     source_node_id)

    source_node_id = node_id
    
    operator = 'Write'

    parameters = get_operator_default_parameters(operator)

    parameters['file'] = 'stack'
    parameters['formatName'] = 'BEAM-DIMAP'

    node_id = 'Write'

    mygraph.add_node(node_id,
                     operator,
                     parameters,
                     source_node_id)

    if show_graph: 
        mygraph.view_graph()

    mygraph.run()
    
def list_bands(product):
    
    reader = ProductIO.getProductReader('BEAM-DIMAP')
    product = reader.readProductNodes(product, None)

    return list(product.getBandNames())


def change_detection(input_product, output_product, expression, show_graph=False):
    
    mygraph = GraphProcessor()
    
    operator = 'Read'

    parameters = get_operator_default_parameters(operator)

    node_id = 'Read'

    source_node_id = ''

    parameters['file'] = input_product

    mygraph.add_node(node_id, operator, parameters, source_node_id)

    source_node_id = node_id 
    
    operator = 'BandMaths'

    parameters = get_operator_default_parameters(operator)

    bands = '''<targetBands>
        <targetBand>
          <name>change_detection</name>
          <type>float32</type>
          <expression>{}</expression>
          <description/>
          <unit/>
          <noDataValue>NaN</noDataValue>
        </targetBand>
        </targetBands>'''.format(expression)

    parameters['targetBandDescriptors'] = bands 

    node_id = 'BandMaths'

    mygraph.add_node(node_id, operator, parameters, source_node_id)

    source_node_id = node_id 
    
    operator = 'Write'

    parameters = get_operator_default_parameters(operator)

    parameters['file'] = output_product
    parameters['formatName'] = 'GeoTIFF-BigTIFF'

    node_id = 'Write'

    mygraph.add_node(node_id,
                     operator,
                     parameters,
                     source_node_id)
    
    if show_graph: 
        mygraph.view_graph()
    
    mygraph.run()

def convert_dim(input_product, show_graph=False):
    
    mygraph = GraphProcessor()
    
    operator = 'Read'

    parameters = get_operator_default_parameters(operator)

    node_id = 'Read'

    source_node_id = ''

    parameters['file'] = input_product

    mygraph.add_node(node_id, operator, parameters, source_node_id)

    source_node_id = node_id 
   
    operator = 'LinearToFromdB'

    node_id = 'LinearToFromdB'
    
    parameters = get_operator_default_parameters(operator)
    
    mygraph.add_node(node_id, operator, parameters, source_node_id)
    
    source_node_id = node_id 
    
    operator = 'Write'

    parameters = get_operator_default_parameters(operator)

    parameters['file'] = input_product.replace('.dim', '_db.tif')
    parameters['formatName'] = 'GeoTIFF-BigTIFF'

    node_id = 'Write'

    mygraph.add_node(node_id, operator, parameters, source_node_id)
    
    if show_graph: 
        mygraph.view_graph()
    
    mygraph.run()

    return input_product.replace('.dim', '_db.tif')
    
def cog(input_tif, output_tif):
    
    translate_options = gdal.TranslateOptions(gdal.ParseCommandLine('-co TILED=YES ' \
                                                                    '-co COPY_SRC_OVERVIEWS=YES ' \
                                                                    ' -co COMPRESS=LZW'))

    ds = gdal.Open(input_tif, gdal.OF_READONLY)

    gdal.SetConfigOption('COMPRESS_OVERVIEW', 'DEFLATE')
    ds.BuildOverviews('NEAREST', [2,4,8,16,32])
    
    ds = None

    ds = gdal.Open(input_tif)
    gdal.Translate(output_tif,
                   ds, 
                   options=translate_options)
    ds = None

    os.remove('{}.ovr'.format(input_tif))
    os.remove(input_tif)

def get_image_wkt(product):
    
    src = gdal.Open(product)
    ulx, xres, xskew, uly, yskew, yres  = src.GetGeoTransform()

    max_x = ulx + (src.RasterXSize * xres)
    min_y = uly + (src.RasterYSize * yres)
    min_x = ulx 
    max_y = uly

    source = osr.SpatialReference()
    source.ImportFromWkt(src.GetProjection())

    target = osr.SpatialReference()
    target.ImportFromEPSG(4326)

    transform = osr.CoordinateTransformation(source, target)

    result_wkt = box(transform.TransformPoint(min_x, min_y)[0],
                     transform.TransformPoint(min_x, min_y)[1],
                     transform.TransformPoint(max_x, max_y)[0],
                     transform.TransformPoint(max_x, max_y)[1]).wkt
    
    return result_wkt


def polygonize(input_tif, band, epsg):
    
    epsg_code = epsg.split(':')[1]
    
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(int(epsg_code))

    source_raster = gdal.Open(input_tif)
    band = source_raster.GetRasterBand(band)
    band_array = band.ReadAsArray()

    out_vector_file = "polygonized.json"

    driver = ogr.GetDriverByName('GeoJSON')

    out_data_source = driver.CreateDataSource(out_vector_file+ "")
    out_layer = out_data_source.CreateLayer(out_vector_file, srs=srs)

    new_field = ogr.FieldDefn('change_detection', ogr.OFTInteger)
    out_layer.CreateField(new_field)

    gdal.Polygonize(band, None, out_layer, 0, [], callback=None )

    out_data_source = None
    source_raster = None

    data = json.loads(open(out_vector_file).read())
    gdf = gp.GeoDataFrame.from_features(data['features'])

    gdf.crs = {'init':'epsg:{}'.format(epsg_code)}
    gdf = gdf.to_crs(epsg=epsg_code)
    
    os.remove(out_vector_file)
    
    return gdf


def create_composite(input_products, output_product, band_expressions):
    
    BandMathX = otbApplication.Registry.CreateApplication("BandMathX")

    BandMathX.SetParameterStringList('il', input_products)
    BandMathX.SetParameterString('out', 'temp_red_green_blue.tif')
    BandMathX.SetParameterString('exp', ';'.join(band_expressions))

    BandMathX.ExecuteAndWriteOutput()

    Convert = otbApplication.Registry.CreateApplication('Convert')

    Convert.SetParameterString('in', 'temp_red_green_blue.tif')
    Convert.SetParameterString('out', output_product)
    Convert.SetParameterString('type', 'linear')
    Convert.SetParameterString('channels', 'rgb')

    Convert.ExecuteAndWriteOutput()

    os.remove('temp_red_green_blue.tif')
    
    return output_product

def create_mask(in_composite, out_mask):
    
    #gdal_calc.py --calc="logical_and(logical_and(A==255, B==0), C==0)" -A $1 --A_band=1 -B $1 --B_band=2 -C $1 --C_band=3 --outfile=${1::-8}.mask.tif
    
    calc_exp="logical_and(logical_and(A==255, B==0), C==0)"
    
    gdalCalc(calc=calc_exp, A=in_composite, A_band=1, B=in_composite, B_band=2, C=in_composite, C_band=3, outfile=out_mask)
    
    
def create_rbb(in_rgb, out_rbb):
    
    #gdal_translate -ot UInt16 -a_nodata 256 ${1::-14}RED-BLUE.rgb.tif ${1::-8}.acd.tif -co COMPRESS=LZW -b 1 -b 3 -b 3
    
    translate_options = gdal.TranslateOptions(gdal.ParseCommandLine('-co COMPRESS=LZW '\
                                                                    '-ot UInt16 ' \
                                                                    '-a_nodata 256 ' \
                                                                    '-b 1 -b 3 -b 3 '))
                                                                    
    ds = gdal.Open(in_rgb, gdal.OF_READONLY)

    gdal.Translate(out_rbb, 
                   ds, 
                   options=translate_options)