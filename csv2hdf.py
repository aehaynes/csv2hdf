import os
from numpy import array
from h5py import File as H5File
import time

def csvTohdf(csv_path, container_names= None, size=2**16, delimiter='\n', overwrite=False, dt='f'):
	''' Converts large .csv files to and Hdf data store of numpy arrays.
		- useful for low RAM environments, where converting large csv's in pandas may be impractical.
		Note: pandas and hdf are compatible, but cumbersome.'''

	start_time = time.time()

	if container_names == None:
		container_names = {'file':'file1','group':'group1','dataset':'dataset1'}
		
	csv_buffer = ''
	out = []
	num_rows = 0
	
	''' create HDF; Check for existing datasets with the 
	same name and overwrite them if overwrite is True '''

	hdf = H5File(container_names['file'])	
	if container_names['group'] in hdf.keys():
		if container_names['dataset'] in hdf[ container_names['group'] ].keys():
			print("Dataset [%s] found in Group [%s]..." % (container_names['dataset'], container_names['group']) )
			if overwrite:
				print("Overwriting Dataset [%s] ..." % (container_names['dataset']) )
				del hdf[ container_names['group'] ][ container_names['dataset'] ]
			else:
				print("Exiting without processing csv.")
				return
	else:
		hdf.create_group(container_names['group'])
	grp = hdf[ container_names['group'] ]
	dataset = grp.create_dataset(container_names['dataset'], (num_rows, 1), maxshape = (None,None) )

	# Process csv to numpy arrays and store in csv
	with open(csv_path) as f:
		f.seek(0)		
		block = csv_buffer + f.read(size)
		while block:
			lines = block.split(delimiter)
			if block[-1] == delimiter:
				csv_buffer = ''
				del lines[-1]
			else:
				csv_buffer = lines.pop()
			out = [ tuple( l.split(',') ) for l in lines]
			num_rows = num_rows + len(out)
			c_row = dataset.shape[0]
			array_out = array(out, dtype=dt)
			dataset.resize((num_rows, array_out.shape[1]))
			dataset[c_row:num_rows] = array_out
			block = csv_buffer + f.read(size)
	return time.time() - start_time