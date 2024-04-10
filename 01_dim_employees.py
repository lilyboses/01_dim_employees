#!/usr/bin/env python
# coding: utf-8

# In[7]:


import pandas as pd
from sqlalchemy import create_engine, text


# In[3]:


source_csv_url = "https://isba-salesorders.s3.amazonaws.com/Employees.csv"

source_csv_df = pd.read_csv(source_csv_url)


# In[4]:


source_csv_df


# In[5]:


raw_host = "isba-dev-01.cmhtgvzs0rf5.us-east-1.rds.amazonaws.com"
raw_username = "admin"
raw_password = "isba_4715"
raw_schema = "raw_SalesOrders"

raw_db_config = {
    "host": raw_host,
    "username": raw_username,
    "password": raw_password,
    "schema": raw_schema

}

# driver://username:password@host/database
raw_engine = create_engine(f"mysql+mysqldb://{raw_db_config['username']}:{raw_db_config['password']}@{raw_db_config['host']}/{raw_db_config['schema']}")


# In[6]:


raw_table = "raw_Employees"

source_csv_df.to_sql(raw_table, raw_engine, index=False, if_exists="append")


# In[8]:


raw_query = text(f'''
SELECT *    
FROM {raw_table}
WHERE inserted_at = (
    SELECT MAX(inserted_at)
    FROM {raw_table}
);      
''')

raw_df = pd.read_sql(raw_query, raw_engine)


# In[9]:


raw_df

