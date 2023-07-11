import psycopg2
import pymssql
from elasticsearch import Elasticsearch


class db_con():
    def rds(self):
        print("rds")
        ###################Dev########################
        connection = psycopg2.connect(host = 'mip-cv-appdata.cfz9blub9l7s.ap-south-1.rds.amazonaws.com',port = 5432,user = 'dev_cv', password = 'Dev$#CV',database='manu_cv_dev')
        ##################Prod##########################
        #connection = psycopg2.connect(host = 'mip-cv-appdata.cfz9blub9l7s.ap-south-1.rds.amazonaws.com',port = 5432,user = 'admin_cv', password = 'Adm!nM@nuCv@123',database='manu_cv')
        return connection
    
    
    def elk(self):
        ###################Dev########################
        es = Elasticsearch(["http://elastic:elastic@172.31.154.203:9200/"])
        ##################Prod##########################
        #es = Elasticsearch(["http://elastic:elastic@172.31.158.159:9200/"])
        return es
    
    def rds_user_data(self):
        ###################Dev########################
        connection = psycopg2.connect(host = 'mip-cv-appdata.cfz9blub9l7s.ap-south-1.rds.amazonaws.com',port = 5432,user = 'dev_cv', password = 'Dev$#CV',database='manu_cv_dev')
        ##################Prod##########################
        #connection = psycopg2.connect(host = 'mip-cv-authdb.cfz9blub9l7s.ap-south-1.rds.amazonaws.com',port = 5432,user = 'admin_cv', password = 'Adm!nM@nuCv@123',database='manufacturing_db')
        return connection