import pandas as pd
import sqlite3 as sql
from sqlite3.dbapi2 import Cursor
import xlsxwriter
from datetime import datetime

class TransformTables():
    """ This class preprocessed the client data, optimizes memory use,
        creates the necessary transformations to obtain three new 
        dataframes that are saved in the ouput folder as .xlsx format"""
    
    def __init__(self,df):
        self.dfr = df.copy(deep=True)
        self.dfr.drop(columns=['altura','peso'],inplace=True)
        self.dfr['prioridad'].fillna(0,inplace=True)
        self.dfr['telefono'].fillna(0,inplace=True)
        self.dfr['fiscal_id']=self.dfr['fiscal_id'].astype('string').str.upper()
        self.dfr['first_name']=self.dfr['first_name'].astype('string').str.upper()
        self.dfr['last_name']=self.dfr['last_name'].astype('string').str.upper()
        self.dfr['gender']=self.dfr['gender'].str.upper().astype('category')
        self.dfr['fecha_nacimiento']=pd.to_datetime(self.dfr['fecha_nacimiento']) 
        self.dfr['fecha_vencimiento']=pd.to_datetime(self.dfr['fecha_vencimiento'])
        self.dfr['correo']=self.dfr['correo'].astype('string').fillna('').str.upper()
        self.dfr['direccion']=self.dfr['direccion'].astype('string').str.upper()
        self.dfr['estatus_contacto']=self.dfr['estatus_contacto'].fillna('').str.upper().astype('category')
        self.dfr['prioridad']=self.dfr['prioridad'].astype('int8')
        self.dfr['telefono']=self.dfr['telefono'].astype('int')

        self.dfr['age'] = (self.dfr['fecha_vencimiento'].dt.year-self.dfr['fecha_nacimiento'].dt.year).astype('int8')
        self.dfr['age_group']=pd.cut(self.dfr['age'],bins=[0,20,30,40,50,60,1000],labels=[1,2,3,4,5,6])
        self.dfr['delinquency'] = ((datetime.now()-self.dfr['fecha_vencimiento']).dt.days).astype('int16')
        self.dfr['fecha_vencimiento'] = self.dfr['fecha_vencimiento'].dt.strftime('%Y-%m-%d')
        self.dfr['fecha_nacimiento'] = self.dfr['fecha_nacimiento'].dt.strftime('%Y-%m-%d')


        columns = ['fiscal_id', 'first_name', 'last_name', 'gender', 'fecha_nacimiento',
           'fecha_vencimiento', 'deuda', 'direccion', 'correo', 'estatus_contacto',
           'prioridad', 'telefono','age','age_group','delinquency']

        new_columns = ['fiscal_id', 'first_name', 'last_name', 'gender','birth_date',
            'due_date','due_balance','address','email','status','priority',
            'phone','age','age_group','delinquency']

        self.dfr.rename(columns=dict(zip(columns,new_columns)),inplace=True)
        
    def transformed_dataframe(self):
        return self.dfr
        
    def clients(self):        
        clients = self.dfr[['fiscal_id','first_name','last_name',
                 'gender','birth_date','age','age_group',
                 'due_date','delinquency','due_balance','address']]        
        clients.to_excel('./output/clients.xlsx',engine='xlsxwriter',index=False)
        
        return 'clients',clients
    
    def emails(self):       
        emails = self.dfr[['fiscal_id','email','status','priority']]
        emails.to_excel('./output/emails.xlsx',engine='xlsxwriter',index=False) 
        
        return 'emails',emails
    
    def phones(self):
        phones = self.dfr[['fiscal_id','phone','status','priority']]
        phones.to_excel('./output/phones.xlsx',engine='xlsxwriter',index=False)
        
        return 'phones',phones

class DataBase():
    """" This class creates the database 'database.db3' on the file system 
         when the program starts, defines the data types of our tables, 
         and stores the records from the preprocessed dataframe.
         Once the program has been run, the database.db3 is saved with the data on the file system."""

    def __init__(self,name_table,table):
        self.table = table
        self.name_table = name_table
        
    def create_database(self):
        conn = sql.connect('database.db3')
        conn.commit()
        conn.close()
        
    def create_tables(self):
        conn = sql.connect("database.db3")
        cursor = conn.cursor()
        cursor.execute(
            """ CREATE TABLE clients (
                fiscal_id STRING,
                first_name STRING,
                last_name STRING,
                gender STRING,
                birth_date STRING,
                age INT,
                age_group INT,
                due_date DATE,
                delinquency INT,
                due_balance INT,
                address STRING );""")                
        cursor.execute(                
                """CREATE TABLE emails (
                fiscal_id STRING,
                emai STRING,
                status STRING,
                priority INT
                ); """ )
        cursor.execute(                
                """CREATE TABLE phones (
                fiscal_id STRING,
                phone STRING,
                status STRING,
                priority INT
                ); """ )         
        
        conn.commit()
        conn.close()
    
    def insert_data(self):
        conn = sql.connect("database.db3")
        cursor = conn.cursor()
        for i,row in self.table.iterrows():
            row = list(dict(row).values())
            insert_row = f"INSERT INTO {self.name_table} VALUES ({str(row).strip('[]')})"
            print(insert_row)
            cursor.execute(insert_row)
        conn.commit()
        conn.close()


            