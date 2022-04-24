import argparse as agp
import getpass
import Obfuscator as ob
import os

class CLIArgumentsProcessor:
	"""This class processes the arguments from the command line interface (CLI), checks if the necessary arguments have been provided and then returns them as
	a dictionary to the caller"""

	def get_parameters(self)->dict:

		params_dict:dict = None

		try:
			parser:agp.ArgumentParser = agp.ArgumentParser(add_help=True)

			#Add all arguments to the parser
			parser.add_argument("-t"\
								, "--TrustedConnection"\
								, dest="trusted_mode"\
								, action = 'store'\
								, default = False\
								, help="Use the MS SQL Server in Trusted Connection mode (default=false)"\
								, type=bool)
			
			parser.add_argument("-d"\
								, "--DSN"\
								, dest="dsn" \
								, action = 'store'\
								, default = None \
								, help="The MS SQL Server DSN descriptor file"\
								, type=str)
			
			parser.add_argument("-s"\
								, "--DBServer"\
								, dest="server"
								, action = 'store'\
								, default = None\
								, help="The MS SQL Server database server name"\
								, type=str)
			
			parser.add_argument("-n"\
								, "--DBName"\
								, dest="db_name"\
								, action = 'store'\
								, default = None\
								, help="The MS-SQL Server database name"\
								, type=str)

			parser.add_argument("-u"\
								, "--DBUser"\
								, dest="username"\
								, action = 'store'\
								, default = None\
								, help="The MS SQL Server database username"\
								, type=str)

			parser.add_argument("-v"\
								, "--ViewName"\
								, dest="viewname"\
								, action = 'store'\
								, default = "dbo.vw_AllSurveyData"\
								, help="The MS SQL Server view name to be used (e.g. SCHEMA.VIEWNAME)"\
								, type=str)

			parser.add_argument("-f"\
								, "--PersistenceFilePath"\
								, dest="persistencefilepath"\
								, action = 'store'\
								, default = os.getcwd() + os.sep + "surveyStructure_LATEST.pkl"\
								, help="The serialised persistence file path (defaulted to os.getcwd() + os.sep + surveyStructure_LATEST.pkl)"\
								, type=str)

			#Now parse the results
			parse_results:agp.Namespace = parser.parse_args()

			#Certain parameters must be provided by the user. If they are not raise an exception.
			if(parse_results.db_name is None):
				raise Exception("A database name must be provided using the -n or --DBName command line argument")

			if(parse_results.server is None):
				raise Exception("A database server name must be provided using the -s or --DBServer command line argument")

			#When not in trusted mode a username and password must be provided
			if(parse_results.trusted_mode == False):
				if(parse_results.username is None):
					raise Exception("A database username must be provided using the -u or --DBUser command line argument")

				#Prompt for the password and encrypt it. It should not be provided directly in cleartext.
				obfuscator = ob.Obfuscator()
				encrypted_pwd = obfuscator.obfuscate(getpass.getpass('Please enter the database password (no echo)'))
			else:
				#We are in trusted mode but the username is not required, so ignore it.
				if(parse_results.username is not None):
					raise Exception("The trusted connection is being used and the database user name has been provided. The username will be ignored.")

			#Place all the CLI parameters into a dictionary
			params_dict = {
						"trusted_mode": parse_results.trusted_mode,
						"dsn":parse_results.dsn,
						"server":parse_results.server,
						"db_name":parse_results.db_name,
						"username":parse_results.username,
						"password": encrypted_pwd,
						"viewname": parse_results.viewname,
						"persistencefilepath":parse_results.persistencefilepath
				}

		except Exception as e:
			print("CLIArgumentsProcessor:get_parameters()-> A command line arguments processing error has occurred:" + str(e))

		return params_dict