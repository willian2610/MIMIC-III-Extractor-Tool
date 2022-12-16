from sqlalchemy import text
import pandas as pd
from pandas import DataFrame
import pandas.io.sql as psql
from google.cloud import bigquery
from google.oauth2 import service_account

class MimicExtractor:

    def __init__(self, platform="local", user="", password="", database="", host="", schema="", port=None, gcp_credential_file="", gcp_project_id="", gcp_dataset=""):
        '''
        :param str platform: the platform used to connect to the database. SMust be 'local', 'aws' or 'gcp'. Dafault value was set as 'local'.
        :param str user: the database user.
        :param str password: the database user's password.
        :param str database: the name of the database.
        :param str host: the database host name.
        :param str schema: database schema name.
        :param int port: port used to connect to the database.
        :param str gcp_credential_file: [REQUERED WHEN PLATFORM = "GCP"] GCP's Credential file path.
        :param str gcp_project_id: [REQUERED WHEN PLATFORM = "GCP"] GCP's project id.
        :param str gcp_dataset: [REQUERED WHEN PLATFORM = "GCP"] MIMIC-III dataset name on GCP's BigQuery.
        '''
        
        self.platform = platform
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = port
        self.gcp_credential_file = gcp_credential_file
        self.gcp_project_id = gcp_project_id
        self.gcp_dataset = gcp_dataset
        
        if platform.lower() == "local":
            self.engine = self.get_engine(user=user, password=password, database=database, host=host, schema=schema, port=port)
        elif platform.lower() == "aws":
            print("Connecting on AWS")
            self.engine = None
        elif platform.lower() == "gcp":
            print("Connecting on GCP")
            
            if not (gcp_credential_file or gcp_project_id or gcp_dataset):
                raise Exception("You must provede all requised parameters for GCP Platform: gcp_credential_file, gcp_project_id and gcp_dataset")
            
            credentials = service_account.Credentials.from_service_account_file(gcp_credential_file)
            project_id = gcp_project_id
            self.gcp_client = bigquery.Client(credentials=credentials, project=project_id)
        else:
            raise Exception("Platform must be one of the following: 'local', 'aws' or 'gcp'")

    def get_engine(self, user="", password="", database="", host="", schema="", port=None):
        '''
        This function creates a new postgresql engine using SQLAlchemy.

        :param str user: the database user.
        :param str password: the database user's password.
        :param str database: the name of the database.
        :param str host: the database host name.
        :param str schema: database schema name.
        :param int port: port used to connect to the database.
        
        :return: SQLAlchemy Engine Object
        :rtype: sqlalchemy.engine.base.Engine
        '''
        from sqlalchemy import create_engine
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}', 
                                connect_args={'options': '-csearch_path={}'.format(schema)})
        return engine

    def get_data_from_mimic(self, query='', log=False) -> DataFrame:
        if log: 
            print(f"Running query:\n {query}")

        if self.platform.lower() == "local":
            with self.engine.connect() as conn:
                query_result = psql.read_sql(text(query), conn)
        elif self.platform.lower() == "aws":
            query_result = None
        elif self.platform.lower() == "gcp":
            query_result = self.gcp_client.query(query).to_dataframe()
        else:
            raise Exception("Platform must be one of the following: 'local', 'aws' or 'gcp'")

        return query_result

    def get_patients_age(self, hadm_ids=[], log=False):
        '''
        This function calculates true age of patients by using a Hospital Admission ID.
        NOTE: Patients over 89 years old had their age set to 300 years.

        :param list hadm_ids: A list of all hadm_ids of interest. If no hadm_ids is provided, then the search will be performed over all patients present in the database.
        
        :return: Returns a DataFrame with the data returned by the query.
        :rtype: DataFrame
        '''

        where_clause = ""

        if hadm_ids:
            where_clause = f"WHERE a.hadm_id IN ({','.join(map(str, hadm_ids))})"

        if (self.platform=='local'):
            age_query = "DATE_PART('year',a.ADMITTIME) - DATE_PART('year',p.dob)"
            table_prefix=""
        elif (self.platform=='gcp'):
            age_query = "EXTRACT(year FROM a.ADMITTIME) - EXTRACT(year FROM p.dob)"
            table_prefix = f"physionet-data.{self.gcp_dataset}."
        elif (self.platform=='aws'):
            age_query=""
            table_prefix=""
        
        query = f"""
        SELECT 
            p.subject_id,
            a.hadm_id,
            {age_query} as age
        FROM {table_prefix}patients p 
        INNER JOIN {table_prefix}admissions a
        ON p.subject_id = a.subject_id
        {where_clause};
        """

        return self.get_data_from_mimic(query=query, log=log)

    def get_patients_ethnicity(self, subject_ids=[], log=False):
        '''
        This function returns patients ethnicity by using it's subject IDs.
        
        :param list subject_ids: A list of all subject_ids of interest. If no subject_id is provided, then the search will be performed over all subject_ids present in the database.
        
        :return: Returns a DataFrame with the data returned by the query.
        :rtype: DataFrame
        '''

        where_clause = ""

        if subject_ids:
            where_clause = f"WHERE subject_id IN ({','.join(map(str, subject_ids))})"

        if (self.platform=='local'):
            table_prefix=""
        elif (self.platform=='gcp'):
            table_prefix = f"physionet-data.{self.gcp_dataset}."
        elif (self.platform=='aws'):
            table_prefix=""
        
        query = f"""
        SELECT 
            subject_id, ethnicity
        FROM {table_prefix}admissions
        {where_clause};
        """

        return self.get_data_from_mimic(query=query, log=log)

    def get_patients_gender(self, subject_ids=[], log=False):
        '''
        This function returns patients gender by using it's subject IDs.
        
        :param list subject_ids: A list of all subject_ids of interest. If no subject_id is provided, then the search will be performed over all subject_ids present in the database.
        
        :return: Returns a DataFrame with the data returned by the query.
        :rtype: DataFrame
        '''

        where_clause = ""

        if subject_ids:
            where_clause = f"WHERE subject_id IN ({','.join(map(str, subject_ids))})"

        if (self.platform=='local'):
            table_prefix=""
        elif (self.platform=='gcp'):
            table_prefix = f"physionet-data.{self.gcp_dataset}."
        elif (self.platform=='aws'):
            table_prefix=""
        
        query = f"""
        SELECT 
            subject_id, gender
        FROM {table_prefix}patients
        {where_clause};
        """

        return self.get_data_from_mimic(query=query, log=log)

    def get_icu_stays_by_diagnosis(self, diagnosis_term='', log=False) -> DataFrame:
    
        '''
        This function uses a diagnosis term to return all ICU Stays that can relate to that diagnosis.
           
        :param str diagnosis_term: Term used to search for a disgnosis.
           
        :return: Returns a DataFrame with the data returned by the query.
        :rtype: DataFrame
        '''

        if (self.platform=='local'):
            table_prefix=""
        elif (self.platform=='gcp'):
            table_prefix = f"physionet-data.{self.gcp_dataset}."
        elif (self.platform=='aws'):
            table_prefix=""
            
        query = f"""
            SELECT 
                i.row_id, i.subject_id, i.hadm_id, i.icustay_id, i.dbsource, i.first_careunit, i.last_careunit 
                , i.intime, i.outtime, i.los, pat.gender, pat.dob, pat.dod, pat.dod_hosp, pat.dod_ssn
                , pat.expire_flag, d.icd9_code, s_icd_d.short_title, s_icd_d.long_title
            FROM {table_prefix}icustays i 
            LEFT JOIN (
                SELECT * FROM {table_prefix}patients p
            ) AS pat ON i.subject_id = pat.subject_id
            LEFT JOIN (
                SELECT di.subject_id, di.icd9_code FROM {table_prefix}diagnoses_icd di 
            ) AS d ON i.subject_id = d.subject_id
            LEFT JOIN (
                SELECT * FROM {table_prefix}d_icd_diagnoses did 
            ) AS s_icd_d ON d.icd9_code = s_icd_d.icd9_code
            WHERE short_title LIKE '%{diagnosis_term}%' OR long_title LIKE '%{diagnosis_term}%';
        """
        
        return self.get_data_from_mimic(query=query, log=log)
