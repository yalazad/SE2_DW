import os
import pandas as pd
from time import gmtime, strftime
from pathlib import Path  

class FilePathHelper:
	"""Generic file helper class to help with file/path checks and also file creation
	"""

	#Checks if the file specified in the file_path parameter exists
	def check_file_exists(self:object, file_path:str)->bool:
		success:bool = True
		try:
			file = open(file_path)
			file.close()

		except FileNotFoundError:
			success = False

		return success
	
	#Checks if files can be written to the directory specified in the file_path parameter
	def is_file_dir_writable(self:object, file_path:str)->bool:
		success:bool = True
		try:
			if(os.access(os.path.dirname(file_path), os.W_OK)==False):
				success = False
		except FileNotFoundError:
			success = False
				
		return success
	
	#Compares 'result_set_as_df parameter' to the unpickled version of the 'file_path' passed as parameter
	#and returns a boolean indicating whether they are the same
	def is_dataframe_same_as_pickled_file(self:object, result_set_as_df:pd.DataFrame, file_path:str)->bool:
		same_file:bool = False
		
		try:
			unpickled_file_df:pd.DataFrame = pd.read_pickle(file_path)

			if(result_set_as_df.equals(unpickled_file_df) == True):
				same_file = True
		except Exception as excp:
			raise Exception("FilePathHelper:is_dataframe_same_as_pickled_file()-> Unable to read/unpickle the persistence file: ").with_traceback(excp.__traceback__)
		
		return same_file

	#Creates files in the specified format using the dataframe passed as parameter. 
	#If the file already exists in the file_path, the old one is renamed and moved to an archive directory before creating the new one.
	def create_file_from_df(self:object, dataframe:pd.DataFrame, file_path:str, format:str)->None:
		#String constant for the archive directory
		ARCHIVE_DIR_PATH:str = os.getcwd() + os.sep + "archive"

		#If we are allowed to write files to the specified file_path
		if(self.is_file_dir_writable(file_path) == True):			
			#Check if there is an existing file, if so then rename the existing file to remove 'LATEST', add the current timestamp and move to the archive dir
			if(self.check_file_exists(file_path) == True):
				#Check if archive directory already exists
				if(os.path.isdir(ARCHIVE_DIR_PATH) == False):
					#It doesn't so create it
					try:
						os.mkdir(ARCHIVE_DIR_PATH)
					except OSError:
						print ("FilePathHelper:create_file_from_df()-> Creation of the directory %s failed" % ARCHIVE_DIR_PATH)
					else:
						print ("Successfully created the directory %s " % ARCHIVE_DIR_PATH)

				try:
					#Rename the old file by removing 'LATEST' and adding the current timestamp
					original_file_path:str = file_path.replace('LATEST.' + format, \
						strftime('%Y%m%d_%H%M%S', gmtime(os.path.getmtime(file_path))) + '.' + format)

					#Build the destination archive path
					destination_path:str = ARCHIVE_DIR_PATH + os.sep + Path(original_file_path).name

					#Move the old file to the archive directory
					os.rename(file_path, destination_path)
				except Exception as excp:
						raise Exception("FilePathHelper:create_file_from_df()-> Unable to rename file: ").with_traceback(excp.__traceback__)
			
			if(format == 'csv'):	
				try:
					#Save the survey data dataframe to a csv file
					dataframe.to_csv(file_path, index = False)
				except Exception as excp:
					raise Exception("FilePathHelper:create_file_from_df()-> CSV file %s could not be created" % file_path).with_traceback(excp.__traceback__)
				else:
					print("Successfuly created CSV file %s" % file_path)
			elif(format == 'pkl'):
				try:
					#Serialise the survey structure table by using pickle and save to a .pkl file
					dataframe.to_pickle(file_path)
				except Exception as excp:
					raise Exception("FilePathHelper:create_file_from_df()-> Pickle file %s could not be created" % file_path).with_traceback(excp.__traceback__)
				else:
					print("Successfuly created pickle file %s" % file_path)
			else:
				raise Exception("FilePathHelper:create_file_from_df()-> Unknown file format. Exiting.")
		else:					
			raise Exception("FilePathHelper:create_file_from_df()-> File directory not writeable: "+ file_path)

