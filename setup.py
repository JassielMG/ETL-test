from pandas.io.parsers import read_csv
from main import TransformTables
from main import DataBase
import os

if __name__ == '__main__':
    while True:
        try:
            root = str(input('Please write the file path: '))
            df = read_csv(root,sep=';',low_memory=True,
                        dtype={'gender':'category','deuda':'int16'})
            
            dataframe = TransformTables(df)
            dataFrames = [dataframe.clients(),dataframe.emails(),dataframe.phones()]
            
            for i,o in enumerate(dataFrames):
                db = DataBase(*o)
                if i == 0:
                    if 'database.db3' not in os.listdir():
                        print('Creating database...')
                        db.create_database()
                        print('Creating tables...')
                        db.create_tables()
                        db.insert_data()
                    else:
                        db.insert_data()                                           
                else:                   
                    db.insert_data()
                
            print('******************** Data has been loaded succesfully **********************')
            break
        except (FileNotFoundError,TypeError,ValueError,KeyError,IsADirectoryError):
            print('Path or file is not valid !')

    



