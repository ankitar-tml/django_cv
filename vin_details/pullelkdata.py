#import prestodb
#import csv
#import sys
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from datetime import date
from elasticsearch import Elasticsearch, helpers
#from datetime import datetime

from datetime import datetime, timedelta

import sys
sys.path.append("cv_api")
import cv_elk_indices
import db_connection

db_obj=db_connection.db_con()
es=db_obj.elk()

index_obj=cv_elk_indices.elk_indices()
index_name=index_obj.vin_details()

class HiveDataExtractor():

	def elkquery2(self,plant,from_date,to_date,status_name):
		from elasticsearch import Elasticsearch, helpers
		fdate=from_date
		tdate=to_date
		print(plant,fdate,tdate,status_name)
		import requests
		#es = Elasticsearch(["http://elastic:elastic@172.31.154.203:9200/"])
		#es = Elasticsearch(["http://elastic:elastic@172.31.158.159:9200/"])
		if status_name=='OFFTAKE':
			query_asset={
				  "query" : {
						"bool" : {
						"must" : [
							{
							"bool" : {
								"must" : [
								{
									"term" : {
									"status.keyword" : {
										"value" : status_name
									}
									}
								},
								{
									"term" : {
									"plant.keyword" : {
										"value" : plant
									}
									}
								}
								],
								"adjust_pure_negative" : True,
								"boost" : 1.0
							}
							},
							{
							"bool" : {
								"must" : [
								{
									"wildcard" : {
									"vin_number.keyword" : {
										"wildcard" : "*MAT*",
										"boost" : 1.0
									}
									}
								},
								{
									"range" : {
									"offtake_date" : {
										"from" : fdate,
										"to" : tdate,
										"include_lower" : True,
										"include_upper" : True,
										"time_zone" : "Z",
										"boost" : 1.0,
										"format": "yyyy-MM-dd HH:mm:ss"
									}
									}
								}
								],
								"adjust_pure_negative" : True,
								"boost" : 1.0
							}
							}
						],
						"adjust_pure_negative" : True,
						"boost" : 1.0
						}
					},
				"_source" : {
					"includes" : [
					"category",
					"crtd_time",
					"description",
					"model",
					"plant",
					"roll_time",
					"sflt_time",
					"vc_number",
					"vin_number",
					"wtcl_time",
					"status",
					"offtake_date"
					],
					"excludes" : [ ]
				},
				"docvalue_fields" : [
					{
					"field" : "crtd_time",
					"format" : "epoch_millis"
					},
					{
					"field" : "offtake_date",
					"format" : "epoch_millis"
					},
					{
					"field" : "roll_time",
					"format" : "epoch_millis"
					},
					{
					"field" : "sflt_time",
					"format" : "epoch_millis"
					},
					{
					"field" : "wtcl_time",
					"format" : "epoch_millis"
					}
				],
				"sort" : [
					{
					"crtd_time" : {
						"order" : "desc",
						"missing" : "_first",
						"unmapped_type" : "keyword"
					}
					}
				]
				}
		else:
			query_asset={
						"query" : {
							"bool" : {
							"must" : [
								{
								"bool" : {
									"must" : [
									{
										"term" : {
										"plant.keyword" : {
											"value" : plant,
											"boost" : 1.0
										}
										}
									},
									{
										"wildcard" : {
										"vin_number.keyword" : {
											"wildcard" : "*MAT*",
											"boost" : 1.0
										}
										}
									}
									],
									"adjust_pure_negative" : True,
									"boost" : 1.0
								}
								},
								{
								"bool" : {
									"should" : [
									{
										"bool" : {
										"should" : [
											{
											"range" : {
												"crtd_time" : {
												"from" : fdate,
												"to" : tdate,
												"include_lower" : True,
												"include_upper" : True,
												"format": "yyyy-MM-dd HH:mm:ss",
												"boost" : 1.0
												}
											}
											},
											{
											"range" : {
												"sflt_time" : {
												"from" : fdate,
												"to" : tdate,
												"include_lower" : True,
												"include_upper" : True,
												"format": "yyyy-MM-dd HH:mm:ss",
												"boost" : 1.0
												}
											}
											}
										],
										"adjust_pure_negative" : True,
										"boost" : 1.0
										}
									},
									{
										"range" : {
										"roll_time" : {
											"from" : fdate,
											"to" : tdate,
											"include_lower" : True,
											"include_upper" : True,
											"format": "yyyy-MM-dd HH:mm:ss",
											"boost" : 1.0
										}
										}
									},
								{
										"range" : {
										"wtcl_time" : {
											"from" : fdate,
											"to" : tdate,
											"include_lower" : True,
											"include_upper" : True,
											"format": "yyyy-MM-dd HH:mm:ss",
											"boost" : 1.0
										}
										}
									}
									],
									"adjust_pure_negative" : True,
									"boost" : 1.0
								}
								}
							],
							"adjust_pure_negative" : True,
							"boost" : 1.0
							}
						},
						"_source" : {
							"includes" : [
							"category",
							"crtd_time",
							"description",
							"model",
							"plant",
							"roll_time",
							"sflt_time",
							"vc_number",
							"vin_number",
							"wtcl_time",
							"status",
							"offtake_date"
							],
							"excludes" : [ ]
						},
						"docvalue_fields" : [
							{
							"field" : "crtd_time",
							"format" : "epoch_millis"
							},
							{
							"field" : "roll_time",
							"format" : "epoch_millis"
							},
							{
							"field" : "sflt_time",
							"format" : "epoch_millis"
							},
							{
							"field" : "wtcl_time",
							"format" : "epoch_millis"
							},
							{
							"field" : "offtake_date",
							"format" : "epoch_millis"
							}
						],
						"sort" : [
							{
							"crtd_time" : {
								"order" : "desc",
								"missing" : "_first",
								"unmapped_type" : "keyword"
							}
							}
						]
						}
		res_vin=es.search(index=index_name,body=query_asset,size=10000,request_timeout=120)
		dict_vin={}
		list1=[]
		dict_vin['category']=''
		dict_vin['crtd_time']=''
		dict_vin['description']=''
		#dict_vin['line_no']=''
		dict_vin['model']=''
		dict_vin['plant']=''
		dict_vin['roll_time']=''
		dict_vin['sflt_time']=''
		#dict_vin['shop']=''
		dict_vin['vc_number']=''
		dict_vin['vin_number']=''
		dict_vin['status']=''
		dict_vin['wtcl_time']=''
		dict_vin['offtake_date']=''
		x2=res_vin['hits'].get('hits')
		for i in x2:
		    del i['_type']
		    del i['_score']
		    del i['_index']
		    del i['_id']
		    dict_vin=i['_source']
		    list1.append(dict_vin.copy())
		return list1
