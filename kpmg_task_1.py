#!/usr/bin/env python
# coding: utf-8

# # KPMG ONLINE INTERSHIP - TASK 1

# In[1]:


#loading the dataset
import pandas as pd
import numpy as np
import datetime
import dateutil


# In[2]:


# transcation dataset
df = pd.ExcelFile(r"C:\Users\vinsl\Downloads\KPMG_VI_New_raw_data_update_final.xlsx")
transaction = pd.read_excel(df,'Transactions')


# In[3]:


transaction.sample(3)


# In[4]:


#renaming the columns
transaction.columns = ['transaction_id','product_id','customer_id','transaction_date','online_order','online_status','brand','product_line','product_class','product_size','list_price','standard_cost','product_first_sold_date']


# In[5]:


transaction.sample(3)


# In[6]:


#finding the null values
transaction.isnull().sum()


# In[33]:


# counting the no of missing values in each columns
for col in transaction.columns:
    miss = transaction[col].isnull().sum()
    if miss>0:
        print("{} has {} missing value(s)".format(col,miss))
    else:
        print("{} has NO missing value!".format(col))


# In[7]:


#finding the duplicated
transaction.duplicated().sum()


# In[ ]:


# no duplicates are there


# In[8]:


transaction.info()


# In[9]:


type(transaction['transaction_date'])


# In[20]:


#converting  object into integer
transaction['transaction_id'] = pd.to_numeric(transaction['transaction_id'],errors='coerce')
transaction['product_id'] = pd.to_numeric(transaction['product_id'],errors='coerce')
transaction['customer_id'] = pd.to_numeric(transaction['customer_id'],errors='coerce')
transaction['list_price'] = pd.to_numeric(transaction['list_price'],errors='coerce')
transaction['standard_cost'] = pd.to_numeric(transaction['standard_cost'],errors='coerce')


# In[28]:


#converting object to datetime format
transaction['transaction_date'] = pd.to_datetime(transaction['transaction_date'],errors='coerce',format="%Y-%m-%d")
transaction['product_first_sold_date'] = pd.to_datetime(transaction['product_first_sold_date'],errors='coerce',format="%Y-%m-%d")


# In[32]:


transaction.sample(3)


# In[31]:


# Profit earned
transaction['profit'] = transaction['standard_cost'] - transaction['list_price']


# In[13]:


transaction['brand'].unique()


# In[14]:


transaction['brand'].describe()


# In[15]:


transaction['product_line'].unique()


# In[16]:


transaction['product_line'].describe()


# In[17]:


transaction['product_size'].unique()


# In[18]:


transaction['product_size'].describe()


# In[19]:


transaction['product_class'].unique()


# In[20]:


transaction['product_class'].describe()


# In[64]:


#Loading the Customer dataset
df = pd.ExcelFile(r"C:\Users\vinsl\Downloads\KPMG_VI_New_raw_data_update_final.xlsx")
customers_demographic = pd.read_excel(df,'CustomerDemographic')


# In[65]:


customers_demographic.sample(3)


# In[66]:


#renaming the columns
customers_demographic.columns = ['customer_id','First_name','Last_name','Gender','past_years_bike_purchase','DOB','Job_title','Job_Category','wealth_segment','deceased_indicator','default','oown_car','tenure']


# In[67]:


customers_demographic.sample(3)


# In[68]:


#null value
customers_demographic.isnull().sum()


# In[69]:


for col in customers_demographic.columns:
    miss = customers_demographic[col].isnull().sum()
    if miss>0:
        print("{} has {} missing value(s)".format(col,miss))
    else:
        print("{} has NO missing value!".format(col))


# In[70]:


#duplicated columns
transaction.duplicated().sum()


# In[71]:


customers_demographic.info()


# In[72]:


#converting object datatype to numeric
customers_demographic['customer_id'] = pd.to_numeric(customers_demographic['customer_id'],errors='coerce')
customers_demographic['past_years_bike_purchase'] = pd.to_numeric(customers_demographic['past_years_bike_purchase'],errors='coerce')
customers_demographic['tenure'] = pd.to_numeric(customers_demographic['tenure'],errors='coerce')


# In[73]:


#Converting object datatype into date
customers_demographic['DOB'] = pd.to_datetime(customers_demographic['DOB'],errors='coerce',format="%Y-%m-%d")


# In[74]:


#dropping default column
customers_demographic.drop('default',axis=1,inplace=True)


# In[47]:


#Loading the Customer address dataset
df = pd.ExcelFile(r"C:\Users\vinsl\Downloads\KPMG_VI_New_raw_data_update_final.xlsx")
customer_address = pd.read_excel(df,'CustomerAddress')


# In[48]:


customer_address.sample(5)


# In[49]:


customer_address.columns = ['customer_id','address','postcode','state','country','property_valuation']


# In[50]:


customer_address.sample(3)


# In[51]:


customer_address.isnull().sum()


# In[52]:


customer_address.duplicated().sum()


# In[53]:


customer_address.info()


# In[55]:


customer_address['customer_id'] = pd.to_numeric(customer_address['customer_id'],errors='coerce')
customer_address['postcode'] = pd.to_numeric(customer_address['postcode'],errors='coerce')
customer_address['property_valuation'] = pd.to_numeric(customer_address['property_valuation'],errors='coerce')


# In[57]:


customer_address['state'].unique()


# In[75]:


#merging transaction and customer dataset into df_merge
df_merge = pd.merge(transaction,customers_demographic,on='customer_id',how='inner')


# In[76]:


#merging df_merge with customer address
df_merge = pd.merge(df_merge,customer_address,on='customer_id',how='inner')


# In[78]:


df_merge.sample(3)


# In[79]:


df_merge['country'].unique()


# In[80]:


#since country columns as only 1 entry(Austraila), we are deleting the column
df_merge.drop('country',inplace=True,axis=1)


# In[112]:


df_merge['state'].unique()


# In[113]:


#state both VIC and Victoria is same likewies NSW and New South Wales are same
df_merge['state'].replace('Victoria','VIC',regex=True,inplace=True)
df_merge['state'].replace('New South Wales','NSW',regex=True,inplace=True)


# In[88]:


df_merge['Job_Category'].unique()


# In[87]:


df_merge['Job_title'].unique()


# In[114]:


df_merge.isnull().sum()


# In[119]:


#finding percentage of missing values
percent_missing = df_merge.isnull().sum() * 100 / len(df_merge)
missing_value_df = pd.DataFrame({'column_name': df_merge.columns,
'percent_missing': percent_missing})
print(missing_value_df.sort_values('percent_missing'))


# In[128]:


#lets delete columns that are less than 1% missing values

index_used =['postcode','past_years_bike_purchase','transaction_id','property_valuation','product_id','customer_id','transaction_date','list_price','product_size',
        'product_class','product_line','brand','standard_cost','profit','product_first_sold_date']
df_merge.dropna(subset=index_used,axis=0,inplace=True)


# In[129]:


df_merge.isnull().sum()


# In[130]:


df_merge.sample(3)


# In[ ]:


#dataset is now cleaned, standarised and transformed.... Volia...!

