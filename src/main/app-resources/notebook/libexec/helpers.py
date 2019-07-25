import cioppy
import geopandas as gp
import os
import pandas as pd
from py_snap_helpers import *
from shapely.geometry import box

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

def bbox_to_wkt(bbox):
    
    return box(*[float(c) for c in bbox.split(',')]).wkt


def pre_process(products, aoi, utm_zone, polarization=None, orbit_type=None, show_graph=False):

    #mygraph = GraphProcessor()
    
    for index, product in products.iterrows():

        mygraph = GraphProcessor()
        
        operator = 'Read'
        parameters = get_operator_default_parameters(operator)
        node_id = 'Read-{0}'.format(index)
        source_node_id = ''
        parameters['file'] = product.local_path
        mygraph.add_node(node_id,
                         operator, 
                         parameters,
                         source_node_id)

        source_node_id = node_id

        operator = 'Subset'
        
        node_id = 'Subset-{0}'.format(index)
        
        parameters = get_operator_default_parameters(operator)
        parameters['geoRegion'] = aoi
        parameters['copyMetadata'] = 'true'

        mygraph.add_node(node_id,
                         operator,
                         parameters,
                         source_node_id)

        source_node_id = node_id
        
        
        operator = 'Apply-Orbit-File'

        parameters = get_operator_default_parameters(operator)
        
        if orbit_type == 'Restituted':
        
            parameters['orbitType'] = 'Sentinel Restituted (Auto Download)'
            

        node_id = 'Apply-Orbit-File-{0}'.format(index)
        mygraph.add_node(node_id, 
                         operator, 
                         parameters, 
                         source_node_id)

        source_node_id = node_id

        operator = 'ThermalNoiseRemoval'
        node_id = 'ThermalNoiseRemoval-{0}'.format(index)
        parameters = get_operator_default_parameters(operator)
        mygraph.add_node(node_id,
                         operator,
                         parameters,
                         source_node_id)

        source_node_id = node_id

        operator = 'Calibration'
        node_id = 'Calibration-{0}'.format(index)
        parameters = get_operator_default_parameters(operator)

        parameters['outputSigmaBand'] = 'true'
        
        if polarization is not None:
            
            parameters['selectedPolarisations'] = polarization
        
        mygraph.add_node(node_id,
                         operator,
                         parameters,
                         source_node_id)

        source_node_id = node_id

        operator = 'Terrain-Correction'
    
        node_id = 'Terrain-Correction-{0}'.format(index)
    
        parameters = get_operator_default_parameters(operator)

        map_proj = utm_zone

        parameters['mapProjection'] = map_proj
        parameters['pixelSpacingInMeter'] = '100.0'            
        parameters['nodataValueAtSea'] = 'false'
        parameters['demName'] = 'SRTM 1Sec HGT'
        
        mygraph.add_node(node_id,
                         operator,
                         parameters,
                         source_node_id)

        source_node_id = node_id

        
        operator = 'Write'

        parameters = get_operator_default_parameters(operator)

        parameters['file'] = product.identifier
        parameters['formatName'] = 'BEAM-DIMAP'

        node_id = 'Write-{0}'.format(index)

        mygraph.add_node(node_id,
                         operator,
                         parameters,
                         source_node_id)
       
        if show_graph: 
            mygraph.view_graph()
        
        mygraph.run()
        

