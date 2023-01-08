from pymongo import MongoClient, ASCENDING , DESCENDING
import pandas as pd

class DB_mongo():

    _CLIENT = MongoClient('localhost' , 27017)
    _DBS = _CLIENT.list_database_names()
    
    print(f'Databases available: {_DBS}')
    print(
    """
    1- create a client: client = DB_Mongo._CLIENT.<db name>'
    2- create an instance: DB_Mongo(client)
    """)
    
    # __FIN = __CLIENT.db_fin
    
    def __init__(self, client: str):
        exec(f'self.client = DB_mongo._CLIENT.{client}')
        # self.db = db

        for i in self.client.list_collection_names():
            print(f'Initializing {i} table')
            exec(f'self.{i} = self.client.{i}')
            


    @property
    def all_tables(self) -> list:
        tables = self.client.list_collection_names()
        return tables
        
    def get_df(self, tbl: classmethod , **kwargs) -> pd.DataFrame:

        """"
        example: 
        db_fin = DB_mongo('db_fin')
        db_fin.get_df('cf')
        """
        
        df = pd.DataFrame(list(tbl.find(kwargs)))
        return df

    # def get_unique(self, tbl, field):
    #     """
    #     example: mongo_stock.db.idx_price.distinct('symbol')
    #     """
    #     return tbl.distinct(field)
    

        

print('mongo imported')

if __name__ == '__main__':
    # print(DB_mongo._CLIENT.db_fin.list_collection_names())
    client_fin = DB_mongo._CLIENT.db_fin
    print(client_fin)
    db_fin = DB_mongo(client_fin)
    print(db_fin)
    print(f'tabes are {db_fin.all_tables}')


