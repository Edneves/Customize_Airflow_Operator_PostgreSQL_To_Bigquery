# Customize_Operator_PostgreSQL_To_Bigquery

 A Classe foi desenvolvida com o intuito de extrair os dados de tabelas de uma banco de dados PostgreSQL de forma full,
também é possível limitar a quantidades de linhas simultâneas a serem inseridas no BigQuery.


1. Armazenar o arquivo "operator.py" no diretório que hospeda os operadores customizados do Airflow.
2. Instalar as bibliotecas ["psycopg2","pandas","oauth2"]
3. Instanciar a classe na DAG como um operator, passando os parâmetros.
4. Modelo:

#
        extract = Extract_PostgreSQL_To_BigQuery(
          task_id=f'extract_data_{table_id}',
          db=database,
          user=user,
          password=password,
          host=host,
          port=port,
          listColumns=listColumns,
          queryTable=query_table,
          granularity=granularity,
          nr_rows=nr_rows,
          column_movto=column_movto,
          credential=credential,
          table_id=table_name,
          project_id=project_id
        
    )

1. "listColumns" = lista das colunas que irá compor o dataframe;
2. "queryTable" = Consulta que será executada no database;
3. "granularity" = Caso queira inserir os dados por partes no BigQuery, sinalizar "True";
4. "nr_rows" = Limita a quantidade de linhas que será composto o dataframe, somente se "granularity=True";
5. "column_movto" = Coluna destinada ao controle de atualização no bigquery, estará em Timestamp;
6. "credential" = Conta de serviço destinada a autenticação no ambiente GCP;
7. "table_id" = Nome da tabela no BigQuery;
8. "project_id" = Id do projeto na GCP;


- Tools used:
1. Python
2. Airflow
3. PostgreSQL
4. BigQuery
