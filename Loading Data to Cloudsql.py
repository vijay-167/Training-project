#!/usr/bin/env python
# coding: utf-8

# In[69]:


#import pandas library as pd
import pandas as pd


# In[70]:


#read csv file and convert into dataframe
data = pd.read_csv('data.csv', encoding ="ISO-8859-1")
df = pd.DataFrame(data)


# In[71]:


#list first 5 records
df.head()


# In[72]:


pip install mysql-connector


# In[73]:


#import libraries
import mysql.connector


# In[74]:


import mysql
from google.cloud import storage 
import numpy as np
from datetime import datetime
import sys


# In[75]:


#import libraries
import mysql.connector


# In[98]:


#Establish connection between python and mysql

cnx = mysql.connector.connect(user='root', password='12345678', host='34.170.212.91', 
                              database='customer')

cursor = cnx.cursor()


# In[10]:


#Create customer_details table in mysql database

cursor.execute('''CREATE table customer_details (
               InvoiceNo varchar(25),
               StockCode varchar(50), 
               Description varchar(100), 
               Quantity int, 
               InvoiceDate varchar(25),
               UnitPrice float, 
               CustomerID float, 
               Country varchar(50)
              )''')


# In[90]:


df.columns


# In[91]:


#check how many nan are present in description column
count_nan = df.isna().sum()
print(count_nan)


# In[92]:


#replace nan with zero to load data into sql table (sql doesnt accept nan)

df['CustomerID']=df['CustomerID'].fillna(0)


# In[93]:


count_nan = df['CustomerID'].isna().sum()
print(count_nan)


# In[94]:


df['Description']=df['Description'].fillna(0)


# In[95]:


count_nan = df['Description'].isna().sum()
print(count_nan)


# In[96]:


#verifying if NAN is still present in the dataframe 

count_nan = df.isna().sum()
print(count_nan)


# In[52]:


for row in df.itertuples():
    cursor.execute('''
    Insert into customer.customer_details(InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)
    values(%s, %s, %s, %s,%s, %s, %s, %s)
   ''',
                  (row.InvoiceNo,
                  row.StockCode,
                  row.Description,
                  row.Quantity,
                  row.InvoiceDate,
                  row.UnitPrice,
                  row.CustomerID,
                  row.Country)
                  )
   
cnx.commit()


# In[ ]:


temp = 0
for i in range(len(df)):
    InvoiceNo = str(df['InvoiceNo'].iloc[i])
    StockCode = str(df['StockCode'].iloc[i])
    Description = str(df['Description'].iloc[i])
    Quantity = int(df['Quantity'].iloc[i])
    InvoiceDate = str(df.iloc[i]['InvoiceDate'])
    UnitPrice = float(df['UnitPrice'].iloc[i])
    CustomerID = float(df['CustomerID'].iloc[i])
    Country = str(df['Country'].iloc[i])

    
    query2 = '''INSERT INTO customer_details (InvoiceNo, StockCode, Description, Quantity, InvoiceDate,UnitPrice, CustomerID, Country)
             values (%s, %s, %s, %s,%s, %s, %s, %s)'''
    val = (InvoiceNo, StockCode, Description, Quantity, InvoiceDate,UnitPrice, CustomerID, Country)
    cursor.execute(query2, val)
    cnx.commit()
    temp = temp + 1


# In[ ]:




