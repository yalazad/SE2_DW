##########################################################   INSTALL PACKAGE DEPENDENCIES   ###################################################################
#Install any missing packages automatically
print("Installing missing packages...")
import os
string = "python setup.py install"
os.system(string)

#Update the sys.path after package installation
print("Updating the system path...")
import site
from importlib import reload
reload(site)

####################################################################   MAIN PROGRAM   #########################################################################
import pandas as pd
import QuerySelector as qs
import FilePathHelper as fph
import CLIArgumentsProcessor as clip
from DBConnection import DBConnection

#String constant for the allSurveyData csv file 
ALL_SURVEY_DATA_FILE_PATH = os.getcwd() + os.sep + "allSurveyData_LATEST.csv"

#Prints a description of the program to the user
def print_prog_dscp()->None:
    print("*************************************************************************************************************************************")
    print("THIS SCRIPT EXTRACTS SURVEY DATA FROM THE SAMPLE DATABASE: Survey_Sample_A19 AS SEEN IN THE S20 CLASS")
    print("IT REPLICATES THE BEHAVIOUR OF THE STORED FUNCTION:dbo.fn_GetAllSurveyDataSQL AND TRIGGER: trg_refreshSurveyView IN PROGRAMMATIC FORM")
    print("*************************************************************************************************************************************")

def main():

	#Print program description
	print_prog_dscp()

	cli_args:dict = None
	try:
		#Process the CLI parameters entered by the user
		cli_args_proc:clip.CLIArgumentsProcessor = clip.CLIArgumentsProcessor()
		cli_args:dict = cli_args_proc.get_parameters()
	except Exception as excp:
		print("Problem processing command line arguments. Exiting"+ str(excp))
		return

	#If we have CLI arguments then try and establish a connection with the database by creating a DBConnection object 
	#and then get the connection to execute queries
	if(cli_args is not None):
		try:
			survey_structure_df:pd.DataFrame = None

			connector = DBConnection(dsn = cli_args["dsn"] \
									, server = cli_args["server"] \
									, db_name = cli_args["db_name"] \
									, username = cli_args["username"] \
									, password = cli_args["password"] \
									, trusted_mode = cli_args["trusted_mode"] \
									, viewname = cli_args["viewname"])

			#If we have a connection to the database then execute the SurveyStructure query
			#and retrieve the results into a pandas dataframe
			if(connector.is_connected()):
				query_selector = qs.QuerySelector(connector.get_connection())
				survey_structure_df:pd.DataFrame = query_selector.get_survey_structure_as_df()
			else:
				raise Exception("No Database connection.").with_traceback(excp.__traceback__)

			#Create a FilePathHelper object to help with file/path processing
			file_path_helper = fph.FilePathHelper()

			#Initialise the booleans to indicate whether or not we need to create the .csv or .pkl files
			create_all_survey_data_file = False
			create_persistence_file = False

			#Check if the pickled persistence file already exists
			if(file_path_helper.check_file_exists(cli_args["persistencefilepath"]) == False):
				#Pickle file does not exist so set boolean create_persistence_file to True so that it can be created
				create_persistence_file	= True							

				#Check if the pivoted survey data csv file exists, if not set create_all_survey_data_file boolean to True so that it can be created
				if(file_path_helper.check_file_exists(ALL_SURVEY_DATA_FILE_PATH) == False):					
					create_all_survey_data_file = True
			else:
				#The pickled SurveyStructure file already exists. Compare it to the dataframe just retrieved (survey_structure_df)
				if(file_path_helper.is_dataframe_same_as_pickled_file(survey_structure_df, cli_args["persistencefilepath"]) == True):
					#The comparison is the same so no change in the SurveyStructure table

					#Check if the all survey data csv file exists
					if(file_path_helper.check_file_exists(ALL_SURVEY_DATA_FILE_PATH) == False):	
						#File does not exist so set create_all_survey_data_file to True to create it.
						create_all_survey_data_file = True
					else:
						#Otherwise both the pickle and csv files exist, there has been no change so notify the user
						print("The SurveyStructure table has not changed from its last known structure therefore there is nothing to do.")
				else:
					#The SurveyStructure table has changed so set boolean create_persistence_file to True so that the latest .pkl can be persisted		
					create_persistence_file	= True	

					#Emulate the trigger behaviour to get the latest all survey data into a csv file as SurveyStructure has changed
					#Set the 'create_all_survey_data_file' boolean to True so that the file will be created
					create_all_survey_data_file = True

			#Create the necessary files
			if(create_persistence_file == True):
				#Create the persistence file
				file_path_helper.create_file_from_df(survey_structure_df, cli_args["persistencefilepath"] , 'pkl')

			if(create_all_survey_data_file == True):
				all_survey_data_df:pd.DataFrame = None
				#Execute the query and get the result set as a dataframe
				if(connector.is_connected()):
					all_survey_data_df = query_selector.get_all_survey_data_as_df()
				else:
					raise Exception("No Database connection").with_traceback(excp.__traceback__)
				#Create the csv file
				file_path_helper.create_file_from_df(all_survey_data_df, ALL_SURVEY_DATA_FILE_PATH , 'csv')
			
		except Exception as e:
			print("Database connection error:" + str(e))
	else:
		print("Error: The command line argument dictionary is None. Exiting")

if __name__ == '__main__':
	main()
	
