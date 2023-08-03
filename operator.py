from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from google.oauth2 import service_account
import psycopg2
import pandas as pd


class Extract_PostgreSQL_To_BigQuery(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            db: str,
            user: str,
            password: str,
            host: str,
            port: str,
            listColumns: str,
            queryTable: str,
            granularity: str,
            nr_rows: int,
            column_movto: str,
            credential: str,
            table_id: str,
            project_id: str,

            **kwargs
    ) -> None:
        super(Extract_PostgreSQL_To_BigQuery, self).__init__(**kwargs)
        self.db = db
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.listColumns = listColumns
        self.queryTable = queryTable
        self.granularity = granularity
        self.nr_rows = nr_rows
        self.column_movto = column_movto
        self.credential = credential
        self.table_id = table_id
        self.project_id = project_id
    
    def execute(self, context):
        try:
            conn = psycopg2.connect(
                database = self.db,
                user = self.user,
                password = self.password,
                host = self.host,
                port = self.port
            )

            cursor = conn.cursor()
            cursor.execute(self.queryTable)
            data = cursor.fetchall()
            conn.close()
        except Exception as e:
            print(f"Error in eextract data: {e}")
        
        try:
            results = []
            nr_rows = 0
            qtd_insert = 0

            credentials = service_account.Credentials.from_service_account_file(
                f'{self.credential}'
            )

            scoped_credentials = credentials.with_scopes(
                ['https://www.googleapis.com/auth/cloud-platform']
            )

            if self.granularity == False:
                df = pd.DataFrame(data, columns=self.listColumns)
                df[self.column_movto] = pd.Timestamp.now(tz='UTC')
                df.to_gbq(self.table_id, 
                          project_id=self.project_id, 
                          if_exists='append', 
                          credentials=scoped_credentials, 
                          location='southamerica-east1')
                print(f"EXECUTED INSERTION THE {df['table_name'].count()} ROWS")
                del df
            else:
                for i in data:
                    nr_rows += 1
                    results.append(i)
                    if len(results) >= self.nr_rows or len(data) == nr_rows:
                        df = pd.DataFrame(results, columns=self.listColumns)
                        df[self.column_movto] = pd.Timestamp.now(tz='UTC')
                        qtd_insert += df['table_name'].count()
                        df.to_gbq(self.table_id, 
                          project_id=self.project_id, 
                          if_exists='append', 
                          credentials=scoped_credentials,
                          location='southamerica-east1')
                        del df
                        print(f"EXECUTADO A INSERÇÃO DE: -> {qtd_insert} LINHAS!")
                        if qtd_insert == len(data):
                            print(f"EXTRACT ALL DATA TABLE!!")
                        results = []
        except Exception as e:
            print(f"Error in migration data: {e}")
