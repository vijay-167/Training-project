from airflow import DAG
from datetime import datetime
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator


dag= DAG(dag_id= 'load_sql', start_date = datetime.today(), catchup=False, schedule_interval='@once')

query1 = '''
         SELECT InvoiceNo, StockCode, Description, Quantity, PARSE_DATETIME('%m/%d/%Y %H:%M', InvoiceDate) AS InvoiceDate, UnitPrice, CustomerID,Country 
            FROM EXTERNAL_QUERY
            (
             "projects/centered-carver-360412/locations/us-central1/connections/bq_sql_connection",
             "SELECT * FROM customer.customer_details"
            );
         '''

sql_load = BigQueryOperator( task_id='sql_load', 
                            destination_dataset_table= "centered-carver-360412.customersql.customer_sql",
                            sql=query1,
                            use_legacy_sql=False,
                            create_disposition="CREATE_IF_NEEDED",
                            write_disposition="WRITE_TRUNCATE",
                            bigquery_conn_id='bigquery_default',
                            dag=dag)



query2 = '''
           delete from `centered-carver-360412.customersql.customer_sql`
           where Quantity < 0 or CustomerID is null ;
         '''

task2 = BigQueryOperator( task_id='task2', 
                            sql=query2,
                            use_legacy_sql=False,
                            bigquery_conn_id='bigquery_default',
                            dag=dag)


query3 = '''
            delete from `customersql.customer_sql` 
            where 
            EXTRACT(YEAR FROM InvoiceDate) = 2011 and 
            EXTRACT(MONTH FROM InvoiceDate) = 12
         '''


task3 = BigQueryOperator( task_id='task3', 
                            sql=query3,
                            use_legacy_sql=False,
                            bigquery_conn_id='bigquery_default',
                            dag=dag)



query4 = '''
            select InvoiceNo, StockCode ,
            Description,Quantity,datetime(InvoiceDate) as InvoiceDate,
            UnitPrice,CustomerID,Country, 
            Quantity * UnitPrice as ItemTotal from `customersql.customer_sql`;
         '''      


ONLINE_RETAIL = BigQueryOperator( task_id='ONLINE_RETAIL', 
                            destination_dataset_table= "centered-carver-360412.customersql.ONLINE_RETAIL",
                            sql=query4,
                            use_legacy_sql=False,
                            create_disposition="CREATE_IF_NEEDED",
                            write_disposition="WRITE_TRUNCATE",
                            bigquery_conn_id='bigquery_default',
                            dag=dag)


   


query5 = '''
            select CustomerID, sum(ItemTotal) as TotalSales, count (Quantity) as OrderCount, 
            avg(ItemTotal) as AvgOrderValue from `customersql.ONLINE_RETAIL`
            group by CustomerID;

         '''


CUSTOMER_SUMMARY = BigQueryOperator( task_id='CUSTOMER_SUMMARY', 
                            destination_dataset_table= "centered-carver-360412.customersql.CUSTOMER_SUMMARY",
                            sql=query5,
                            use_legacy_sql=False,
                            create_disposition="CREATE_IF_NEEDED",
                            write_disposition="WRITE_TRUNCATE",
                            bigquery_conn_id='bigquery_default',
                            dag=dag)



query6 = '''
            select Country, sum (ItemTotal) as TotalSales, sum(Itemtotal)*100/(select sum(ItemTotal)
            from `customersql.ONLINE_RETAIL`) as PercentofCountrySales from `customersql.ONLINE_RETAIL`
            group by country;
         '''


SALES_SUMMARY = BigQueryOperator( task_id='SALES_SUMMARY', 
                            destination_dataset_table= "centered-carver-360412.customersql.SALES_SUMMARY",
                            sql=query6,
                            use_legacy_sql=False,
                            create_disposition="CREATE_IF_NEEDED",
                            write_disposition="WRITE_TRUNCATE",
                            bigquery_conn_id='bigquery_default',
                            dag=dag)








Start = DummyOperator(task_id='Start')
End = DummyOperator(task_id='End')

Start >> sql_load >> task2 >> task3 >> ONLINE_RETAIL >> [CUSTOMER_SUMMARY, SALES_SUMMARY]>> End