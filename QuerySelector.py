import pyodbc
import pandas as pd

class QuerySelector:
	"""This class is used to execute the equivalent of the SQL queries present in dbo.fn_GetAllSurveyDataSQL. The result sets are returned as dataframes to 
	the caller for further processing. The queries are executed using the pyodbc database connection object passed as parameter when instantiating the class."""

	def __init__(self, connection):
		if(connection is not None):
			self._connection = connection
		else:
			raise Exception("QuerySelector:__init__-> DB connection is None").with_traceback(excp.__traceback__)

	# The below function selects the SurveyId and QuestionId from the SurveyStructure table and returns the results as a dataframe
	#This will enable the trigger behaviour from dbo.trg_refreshSurveyView to be replicated in code
	def get_survey_structure_as_df(self)-> pd.DataFrame:
		str_select_survey_struct: str = """ SELECT SurveyId, QuestionId 
									FROM SurveyStructure 
									ORDER BY SurveyId, QuestionId """

		try:
			survey_struct_results:pd.Dataframe = pd.read_sql(str_select_survey_struct, self._connection)
		except Exception as excp:
			raise Exception("QuerySelector:get_survey_structure_as_df() -> Cannot execute query:str_select_survey_struct").with_traceback(excp.__traceback__)

		return survey_struct_results


	# Selects all the survey data as done in stored procedure dbo.getAllSurveyData and returns the results as a dataframe
	def get_all_survey_data_as_df(self)-> pd.DataFrame:
	
		str_query_template_for_answer_column: str = """ COALESCE(
					(
						SELECT a.Answer_Value
						FROM Answer as a
						WHERE
							a.UserId = u.UserId
							AND a.SurveyId = <SURVEY_ID>
							AND a.QuestionId = <QUESTION_ID>
					), -1) AS ANS_Q<QUESTION_ID> """

		str_query_template_for_null_columnn: str = ' NULL AS ANS_Q<QUESTION_ID> '

		str_query_template_outer_union_query: str = """	SELECT UserId
										, <SURVEY_ID> as SurveyId
										, <DYNAMIC_QUESTION_ANSWERS>
										FROM
										[User] as u
										WHERE EXISTS
										(
											SELECT *
											FROM Answer as a
											WHERE u.UserId = a.UserId
											AND a.SurveyId = <SURVEY_ID>
										) """

		str_current_union_query_block: str = ''
		str_final_query: str = ''

		#MAIN LOOP to loop over all the surveys

		#For each survey in current_survey_id, we need to construct the answer column queries
		#inner loop, over the questions of the survey

		#resultset of SurveyId, QuestionId, flag InSurvey indicating whether
		#the question is in the survey structure

		#T-SQL cursors are replaced by a query whose result set is retrieved into a pandas dataframe
		survey_select_statement: str = 'SELECT SurveyId FROM Survey ORDER BY SurveyId'

		try:
			survey_query_df:pd.DataFrame = pd.read_sql(survey_select_statement, self._connection)
		except Exception as excp:
			raise Exception("QuerySelector:get_all_survey_data_as_df() -> Cannot execute query:survey_select_statement").with_traceback(excp.__traceback__)

		current_survey_id: int = None
		for current_survey_row_idx, current_survey_row in survey_query_df.iterrows():
			current_survey_id = current_survey_row["SurveyId"]

			str_question_query: str = """ SELECT *
							FROM
							(
							SELECT
								SurveyId,
								QuestionId,
								1 as InSurvey
							FROM
								SurveyStructure
							WHERE
								SurveyId = {fcurrentSurveyId}
							UNION
							SELECT 
								{fcurrentSurveyId} as SurveyId,
								Q.QuestionId,
								0 as InSurvey
							FROM
								Question as Q
							WHERE NOT EXISTS
							(
								SELECT *
								FROM SurveyStructure as S
								WHERE S.SurveyId = {fcurrentSurveyId} AND S.QuestionId = Q.QuestionId
							)
						) as t
						ORDER BY QuestionId; """.format(fcurrentSurveyId = current_survey_id)
		
			question_query_df:pd.DataFrame = None
			try:
				question_query_df:pd.Dataframe = pd.read_sql(str_question_query, self._connection)
			except Exception as excp:
				raise Exception("QuerySelector:get_all_survey_data_as_df() -> Cannot execute query:str_question_query").with_traceback(excp.__traceback__)

			current_question_id: int = None
			current_in_survey: int = None

			str_columns_query_part: str = ''

			for current_question_row_idx, current_question_row in question_query_df.iterrows():
				#Inner loop iterates over the questions
				#Is the current question (inner loop) in the current survey (outer loop)? 
				current_survey_id_in_question = current_question_row["SurveyId"]
				current_question_id = current_question_row["QuestionId"]
				current_in_survey = current_question_row["InSurvey"]

				if(current_in_survey == 0):#current question is not in the current survey
					str_columns_query_part = str_columns_query_part + \
						str.replace(str_query_template_for_null_columnn,'<QUESTION_ID>',str(current_question_id))
				else:
					str_columns_query_part = str_columns_query_part + \
						str.replace(str_query_template_for_answer_column,'<QUESTION_ID>',str(current_question_id))

				#Place a comma between columns in the select statement, except for the last one
				if(current_question_row_idx+1 < len(question_query_df.index)):
					str_columns_query_part = str_columns_query_part + ' , '

			#Back in the outer loop over surveys
			str_current_union_query_block = str.replace(str_query_template_outer_union_query, \
				'<DYNAMIC_QUESTION_ANSWERS>', str_columns_query_part)

			str_current_union_query_block = str.replace(str_current_union_query_block, \
				'<SURVEY_ID>', str(current_survey_id))

			str_final_query = str_final_query + str_current_union_query_block

			#Place a UNION between queries, except for the last one
			if(current_survey_row_idx+1 < len(survey_query_df.index)):
					str_final_query = str_final_query + ' UNION '
	
		try:
			all_survey_data_results:pd.Dataframe = pd.read_sql(str_final_query, self._connection)
		except Exception as excp:
			raise Exception("QuerySelector:get_all_survey_data_as_df() -> Cannot execute query:str_final_query").with_traceback(excp.__traceback__)
				
		return all_survey_data_results
