import os
from numpy import array, dtype, arange
from h5py import File as H5File
import time


def constr_dtype(names, sample):
	dtyp = []
	dim = 1

	for (n,s) in zip(names, sample):
		''' Specify any custom rules for dtypes here e.g.

		#if name == VariableName:
		#	dt = dtype( VariableType )
		#elif VariableNamePattern in name:
		#	dt = dtype(VariableType )
		#else:
			#dt = dtype('float32') 

		Take care to mind the type, e.g. for arithmetic 
		operations division of integers where a float32
		is expected as the result. '''
		try:
			float(s)
			dtyp.append( (n, dtype('float'), dim) )
			#int(s)
			#dtyp.append( (n, dtype('int'), dim) )
		except:
			dtyp.append( (n, dtype('S16'), dim) )
			#try:
			#	float(s)
			#	dtyp.append( (n, dtype('float'), dim) )
			#except:
				
	return dtype( dtyp )


def csvToHDF(CSVPath, ContainerNames= None, Size=2**16, Delimiter='\n', Overwrite=False, Header=False):
	''' Converts large .csv files to and HDF data store of numpy arrays. 
		Useful for low RAM environments, where converting 
		large csv's in pandas is impractical. 
		
		Note: Pandas and HDF are compatible, but cumbersome.'''

	StartTime = time.time()

	#Get Headers with variable name and set dtype for each variable
	with open(CSVPath) as f:
		Sample = array( f.readline().split(',') )
		NCols = len( Sample )
		N = range(NCols)
		_Vars = array( [ 'var%s'%i for i in N ] )

		if Header == True:
			#strip var names of double quotes and newlines
			Headers = array( [s.replace('"','').replace('\n', '') for s in Sample] ) 
			# Fix for empty string variable names
			Mask = (Headers != '')
			Vars = []
			for i in range(len(Mask)):
				#User variables from Headers if the name is non empty
				if Mask[i]:
					Vars.append( Headers[i] )
				#Otherwise use names in Vars
				else:
					Vars.append( _Vars[i] )
			# Get next line as a sample to determine dtype in next step
			Sample = array( f.readline().split(',') )
		else:
			Vars = _Vars

		#Types = [ dtype( type(v) ) for v in Headers ]
	dt = constr_dtype(Vars, Sample)

	if ContainerNames == None:
		ContainerNames = ['file1','group1','dataset1']

	''' Create HDF; Check for existing datasets with the 
		same name and Overwrite them if Overwrite is True '''

	CSVBuffer = ''
	Out = []
	NumRows = 0
	HDF = H5File( ContainerNames[0] )
	Groups = HDF.keys()
	if ContainerNames[1] in Groups:
		Datasets = HDF[ ContainerNames[1] ].keys()
		if ContainerNames[2] in Datasets:
			print("Dataset [%s] found in Group [%s]..." % 
					(ContainerNames[2], ContainerNames[1]) )
			if Overwrite:
				print("Overwriting Dataset [%s] ..." % (ContainerNames[2]) )
				del HDF[ ContainerNames[1] ][ ContainerNames[2] ]
			else:
				print("Exiting without processing csv.")
				return
	else:
		HDF.create_group( ContainerNames[1] )
	Grp = HDF[ ContainerNames[1] ]
	dataset = Grp.create_dataset( ContainerNames[2], (NumRows, ), dt, maxshape = (None,) )

	# Process csv to numpy arrays and store in csv
	with open(CSVPath) as f:
		f.seek(0)
		if Header == True:
			f.readline()
		Block = CSVBuffer + f.read(Size)
		while Block:
			Lines = Block.split(Delimiter)
			if Block[-1] == Delimiter:
				CSVBuffer = ''
				del Lines[-1]
			else:
				CSVBuffer = Lines.pop()
			Out 	= [ tuple( l.split(',') ) for l in Lines]
			NumRows = NumRows + len(Out)
			RCount	= dataset.shape[0]
			dataset.resize((NumRows, ))
			ArrOut = array(Out, dtype=dt)
			dataset[RCount:NumRows] = ArrOut
			Block 	= CSVBuffer + f.read(Size)
	return time.time() - StartTime



