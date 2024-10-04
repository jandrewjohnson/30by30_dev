import os, sys
import hazelbean as hb

p = hb.ProjectFlow('../')
p.user_dir = os.path.expanduser('~')
p.base_data_dir = os.path.join(p.user_dir, 'Files/base_data')
input_zip_ref_path = os.path.join('seals', 'projects', '30by30_v1.0.0', 'inputs.zip')
p.get_path(input_zip_ref_path, possible_dirs=[p.project_dir], download_destination_dir=p.project_dir, strip_relative_paths_for_output=True, verbose=True)

output_inputs_zip_path = os.path.join(p.project_dir, 'inputs.zip')
hb.unzip_file(output_inputs_zip_path, output_folder=p.project_dir, verbose=True)

print("script complete!")