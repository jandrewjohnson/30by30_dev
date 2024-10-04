import os, sys
from seals import seals_utils
from seals import seals_initialize_project
import hazelbean as hb
import pandas as pd
from seals import seals_tasks
from seals import seals_generate_base_data
from seals import seals_process_coarse_timeseries
from seals import seals_main
from seals import seals_visualization_tasks

from seals import * 

def resample_pa(p):
    hb.log('Resampling PA')
    resampled_global_path = os.path.join(p.cur_dir, 'resampled_pa_global.tif')
    resampled_path = os.path.join(p.cur_dir, 'resampled_pa.tif')
    coarse_path = os.path.join(p.inputs_dir, 'priority_binaries_150sec', 'results_ws_0996_wc_0004_p0.tiff')
    
    if not hb.path_exists(resampled_global_path):
        hb.path_exists(coarse_path, verbose=True)
        hb.resample_to_match(coarse_path, p.ha_per_cell_fine_path, resampled_global_path, output_data_type=1, resample_method='near', ndv=255)
    
    if not hb.path_exists(resampled_path):
        
        # Clip to bb
        hb.clip_raster_by_bb(resampled_global_path, p.bb, resampled_path)

def build_30by30_task_tree(p):

    # Define the project AOI
    p.project_aoi_task = p.add_task(seals_tasks.project_aoi)

    ##### FINE PROCESSED INPUTS #####    
    p.fine_processed_inputs_task = p.add_task(seals_generate_base_data.fine_processed_inputs)
    p.generated_kernels_task = p.add_task(seals_generate_base_data.generated_kernels, parent=p.fine_processed_inputs_task, creates_dir=False)
    p.lulc_clip_task = p.add_task(seals_generate_base_data.lulc_clip, parent=p.fine_processed_inputs_task, creates_dir=False)
    p.lulc_simplifications_task = p.add_task(seals_generate_base_data.lulc_simplifications, parent=p.fine_processed_inputs_task, creates_dir=False)
    p.lulc_binaries_task = p.add_task(seals_generate_base_data.lulc_binaries, parent=p.fine_processed_inputs_task, creates_dir=False)
    p.lulc_convolutions_task = p.add_task(seals_generate_base_data.lulc_convolutions, parent=p.fine_processed_inputs_task, creates_dir=False)
    p.resample_pa_task = p.add_task(resample_pa, parent=p.fine_processed_inputs_task, creates_dir=True)

    ##### COARSE CHANGE #####
    p.coarse_change_task = p.add_task(seals_process_coarse_timeseries.coarse_change, skip_existing=0)
    p.extraction_task = p.add_task(seals_process_coarse_timeseries.coarse_extraction, parent=p.coarse_change_task, run=1, skip_existing=0)
    p.coarse_simplified_task = p.add_task(seals_process_coarse_timeseries.coarse_simplified_proportion, parent=p.coarse_change_task, skip_existing=0)
    p.coarse_simplified_ha_task = p.add_task(seals_process_coarse_timeseries.coarse_simplified_ha, parent=p.coarse_change_task, skip_existing=0)
    p.coarse_simplified_ha_difference_from_previous_year_task = p.add_task(seals_process_coarse_timeseries.coarse_simplified_ha_difference_from_previous_year, parent=p.coarse_change_task, skip_existing=0)

    ##### REGIONAL 
    p.regional_change_task = p.add_task(seals_process_coarse_timeseries.regional_change)     

    ##### ALLOCATION #####
    p.allocations_task = p.add_iterator(seals_main.allocations)
    p.allocation_zones_task = p.add_iterator(seals_main.allocation_zones, run_in_parallel=p.run_in_parallel, parent=p.allocations_task)
    p.allocation_task = p.add_task(seals_main.allocation, parent=p.allocation_zones_task, skip_existing=1)

    ##### STITCH ZONES #####
    p.stitched_lulc_simplified_scenarios_task = p.add_task(seals_main.stitched_lulc_simplified_scenarios)

    ##### VIZUALIZE EXISTING DATA #####
    p.visualization_task = p.add_task(seals_visualization_tasks.visualization)
    p.lulc_pngs_task = p.add_task(seals_visualization_tasks.lulc_pngs, parent=p.visualization_task)
    
 



main = ''
if __name__ == '__main__':

    ### ------- ENVIRONMENT SETTINGS -------------------------------
    # Users should only need to edit lines in this section
    
    # Create a ProjectFlow Object to organize directories and enable parallel processing.
    p = hb.ProjectFlow()

    # Set the project_dir wherever you want, though it requires write-permissions and preferably should not be in a cloud-synced directory.
    p.project_dir = '../'
    # p.base_data_dir = os.path.join(p.project_dir, 'base_data')  
    p.base_data_dir = os.path.join(os.path.expanduser('~')  , 'Files/base_data')

    p.set_project_dir(p.project_dir) 
    
    p.run_in_parallel = 1 # Must be set before building the task tree if the task tree has parralel iterator tasks.

    # Build the task tree via a building function and assign it to p. IF YOU WANT TO LOOK AT THE MODEL LOGIC, INSPECT THIS FUNCTION
    build_30by30_task_tree(p)

    # ProjectFlow downloads all files automatically via the p.get_path() function. If you want it to download from a different 
    # bucket than default, provide the name and credentials here. Otherwise uses default public data 'gtap_invest_seals_2023_04_21'.
    p.data_credentials_path = None
    p.input_bucket_name = None
    
    ## Set defaults and generate the scenario_definitions.csv if it doesn't exist.
    # SEALS will run based on the scenarios defined in a scenario_definitions.csv
    # If you have not run SEALS before, SEALS will generate it in your project's input_dir.
    # A useful way to get started is to to run SEALS on the test data without modification
    # and then edit the scenario_definitions.csv to your project needs.   
    p.scenario_definitions_filename = '30by30_test_scenarios.csv' 
    p.scenario_definitions_path = os.path.join(p.input_dir, p.scenario_definitions_filename)
    seals_initialize_project.initialize_scenario_definitions(p)
        
    # SEALS is based on an extremely comprehensive region classification system defined in the following geopackage.
    global_regions_vector_ref_path = os.path.join('cartographic', 'ee', 'ee_r264_correspondence.gpkg')
    p.global_regions_vector_path = p.get_path(global_regions_vector_ref_path)

    # Set processing resolution: determines how large of a chunk should be processed at a time. 4 deg is about max for 64gb memory systems
    p.processing_resolution = 1.0 # In degrees. Must be in pyramid_compatible_resolutions

    seals_initialize_project.set_advanced_options(p)
    
    p.plotting_level = 111

    p.cython_reporting_level = 111
    p.calibration_cython_reporting_level = 111
    p.output_writing_level = 111  # >=2 writes chunk-baseline lulc
    p.write_projected_coarse_change_chunks = 111  # in the SEALS allocation, for troubleshooting, it can be useful to see what was the coarse allocation input.
    p.write_calibration_generation_arrays = 111  #
    
    p.L = hb.get_logger('test_run_seals')
    hb.log('Created ProjectFlow object at ' + p.project_dir + '\n    from script ' + p.calling_script + '\n    with base_data set at ' + p.base_data_dir)
    
    p.execute()

    result = 'Done!'


