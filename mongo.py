from pymongo import MongoClient, ASCENDING , DESCENDING
import pandas as pd

class DB_mongo():

    _CLIENT = MongoClient('localhost' , 27017)
    _DBS = _CLIENT.list_database_names()
    
    print(_DBS)
    
    # __FIN = __CLIENT.db_fin
    
    def __init__(self, db):
        self.db = db

        for i in self.db.list_collection_names():
            self.i = self.db.i

    @property
    def all_tables(self):
        tables = self.db.list_collection_names()
        return tables
        
    def get_df(self, tbl, **kwargs):

        """"
        example: mongo_stock.get_df(mongo_stock.db.profile)
        """
        
        df = pd.DataFrame(list(tbl.find(kwargs)))
        
        return df


print('mongo imported')