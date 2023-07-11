from django.test import TestCase

# Create your tests here.

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from datetime import date
from elasticsearch import Elasticsearch, helpers
import pandas as pd

from datetime import datetime, timedelta
import collections

import sys
sys.path.append("cv_api")
import cv_elk_indices
import db_connection

db_obj=db_connection.db_con()
es=db_obj.elk()

index_obj=cv_elk_indices.elk_indices()
index_name=index_obj.TAT()
index_dropdown=index_obj.TAT_store_dock()



class elk_data_extract():
	def distinct_store_dock(self):
										from elasticsearch import Elasticsearch, helpers
										import requests
										#es = Elasticsearch(["http://elastic:elastic@172.31.154.203:9200/"])
										#es = Elasticsearch(["http://elastic:elastic@172.31.158.159:9200/"])
										stock_dock={}
										storename={
										"size" : 0,
										"query" : {
											"bool" : {
											"must" : [
												{
												"exists" : {
													"field" : "storename",
													"boost" : 1.0
												}
												},
												{
												"term" : {
													"flag.keyword" : {
													"value" : "store",
													"boost" : 1.0
													}
												}
												}
											],
											"adjust_pure_negative" : True,
											"boost" : 1.0
											}
										},
										"_source" : False,
										"aggregations" : {
											"groupby" : {
											"composite" : {
												"size" : 1000,
												"sources" : [
												{
													"storename" : {
													"terms" : {
														"field" : "storename.keyword",
														"missing_bucket" : True,
														"order" : "asc"
													}
													}
												}
												]
											}
											}
										}
										}
										res_vin=es.search(index=index_dropdown,body=storename,size=10000,request_timeout=120)
										dict_vin={}
										store_list=[]
										x2=res_vin['aggregations'].get('groupby').get('buckets')
										for i in x2:
												dict_vin=i['key']['storename']
												data={}
												data['storename']=i['key']['storename']
												store_list.append(data)
										#return store_list

										dockname={
										"size" : 0,
										"query" : {
											"bool" : {
											"must" : [
												{
												"exists" : {
													"field" : "dockname",
													"boost" : 1.0
												}
												},
												{
												"term" : {
													"flag.keyword" : {
													"value" : "dock",
													"boost" : 1.0
													}
												}
												}
											],
											"adjust_pure_negative" : True,
											"boost" : 1.0
											}
										},
										"_source" : False,
										"aggregations" : {
											"groupby" : {
											"composite" : {
												"size" : 1000,
												"sources" : [
												{
													"dockname" : {
													"terms" : {
														"field" : "dockname.keyword",
														"missing_bucket" : True,
														"order" : "asc"
													}
													}
												},
												{
													"dockno" : {
													"terms" : {
														"field" : "dockno",
														"missing_bucket" : True,
														"order" : "asc"
													}
													}
												}
												]
											}
											}
										}
										}
										res_vin=es.search(index=index_dropdown,body=dockname,size=10000,request_timeout=120)
										dock_list=[]
										x2=res_vin['aggregations'].get('groupby').get('buckets')
										for i in x2:
											dockid_dict={}
											dockname_dict={}
											dockid_dict=i['key']['dockno']
											dockid_dict=str(dockid_dict)
											dockname_dict=i['key']['dockname']
											data={}
											data['dock_name']=dockname_dict
											data['dock_id']=dockid_dict
											dock_list.append(data)
											stock_dock['dock']=dock_list
											stock_dock['store']=store_list
										return stock_dock
	#detailed View click
	def detailedview(self,dock_name,store_name,from_date,to_date):
					from elasticsearch import Elasticsearch, helpers
					import requests
					#es = Elasticsearch(["http://elastic:elastic@172.31.154.203:9200/"])
					#es = Elasticsearch(["http://elastic:elastic@172.31.158.159:9200/"])
					if dock_name!='undefined' and store_name!='undefined':
						count_query={
								"size" : 0,
								"query" : {
									"bool" : {
									"must" : [
										{
										"bool" : {
											"must" : [
											{
												"term" : {
												"dockid" : {
													"value" : dock_name,
													"boost" : 1.0
												}
												}
											},
											{
												"term" : {
												"storename.keyword" : {
													"value" : store_name,
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
											"slip_date" : {
											"from" : from_date,
											"to" : to_date,
											"include_lower" : True,
											"include_upper" : True,
											"time_zone" : "Z",
											"boost" : 1.0
											}
										}
										}
									],
									"adjust_pure_negative" : True,
									"boost" : 1.0
									}
								},
								"_source" : False,
								"sort" : [
									{
									"_doc" : {
										"order" : "asc"
									}
									}
								],
								"track_total_hits" : 2147483647
								}
						search_df=es.search(index=index_name,body=count_query,request_timeout=120)
						count=search_df['hits']['total']['value']
						query={
							"size" : count,
							"query" : {
								"bool" : {
								"must" : [
									{
									"bool" : {
										"must" : [
										{
											"term" : {
											"dockid" : {
												"value" : dock_name,
												"boost" : 1.0
											}
											}
										},
										{
											"term" : {
											"storename.keyword" : {
												"value" : store_name,
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
										"slip_date" : {
										"from" : from_date,
										"to" : to_date,
										"include_lower" : True,
										"include_upper" : True,
										"time_zone" : "Z",
										"boost" : 1.0,
										"format":"yyyy-MM-dd"
										}
									}
									}
								],
								"adjust_pure_negative" : True,
								"boost" : 1.0
								}
							},
							"_source" : False,
							"fields" : [
								{
								"field" : "veh_regn_no"
								},
								{
								"field" : "slip_no"
								},
								{
								"field" : "slip_date",
								"format" : "yyyy-MM-dd"
								},
								{
								"field" : "leci_no"
								},
								{
								"field" : "quality_ind"
								},
								{
								"field" : "storename"
								},
								{
								"field" : "dockname"
								},
								{
								"field" : "vs_dockcheckintime"
								},
								{
								"field" : "vs_dockcheckouttime"
								},
								{
								"field" : "vehicle_status"
								},
								{
								"field" : "vs_out_in_hrs"
								}
							],
							"sort" : [
								{
								"_doc" : {
									"order" : "asc"
								}
								}
							]
							}
					elif dock_name != "undefined":
						count_query={
								"size" : 0,
								"query" : {
									"bool" : {
									"must" : [
										{
										"bool" : {
											"must" : [
											{
												"term" : {
												"dockid" : {
													"value" : dock_name,
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
											"slip_date" : {
											"from" : from_date,
											"to" : to_date,
											"include_lower" : True,
											"include_upper" : True,
											"time_zone" : "Z",
											"boost" : 1.0,
											"format":"yyyy-MM-dd"
											}
										}
										}
									],
									"adjust_pure_negative" : True,
									"boost" : 1.0
									}
								},
								"_source" : False,
								"sort" : [
									{
									"_doc" : {
										"order" : "asc"
									}
									}
								],
								"track_total_hits" : 2147483647
								}
						search_df=es.search(index=index_name,body=count_query,request_timeout=120)
						count=search_df['hits']['total']['value']
						query={
								"size" : count,
								"query" : {
									"bool" : {
									"must" : [
										{
										"term" : {
											"dockid" : {
											"value" : dock_name,
											"boost" : 1.0
											}
										}
										},
										{
										"range" : {
											"slip_date" : {
											"from" : from_date,
											"to" : to_date,
											"include_lower" : True,
											"include_upper" : True,
											"time_zone" : "Z",
											"boost" : 1.0,
											"format":"yyyy-MM-dd"
											}
										}
										}
									],
									"adjust_pure_negative" : True,
									"boost" : 1.0
									}
								},
								"_source" : False,
								"fields" : [
									{
									"field" : "veh_regn_no"
									},
									{
									"field" : "slip_no"
									},
									{
									"field" : "slip_date",
									"format" : "yyyy-MM-dd"
									},
									{
									"field" : "leci_no"
									},
									{
									"field" : "quality_ind"
									},
									{
									"field" : "storename"
									},
									{
									"field" : "dockname"
									},
									{
									"field" : "vs_dockcheckintime"
									},
									{
									"field" : "vs_dockcheckouttime"
									},
									{
									"field" : "vehicle_status"
									},
									{
									"field" : "vs_out_in_hrs"
									}
								],
								"sort" : [
									{
									"_doc" : {
										"order" : "asc"
									}
									}
								]
								}
					elif store_name!= "undefined":
						count_query={
								"size" : 0,
								"query" : {
									"bool" : {
									"must" : [
										{
										"bool" : {
											"must" : [
											{
												"term" : {
												"storename.keyword" : {
													"value" : store_name,
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
											"slip_date" : {
											"from" : from_date,
											"to" : to_date,
											"include_lower" : True,
											"include_upper" : True,
											"time_zone" : "Z",
											"boost" : 1.0,
											"format":"yyyy-MM-dd"
											}
										}
										}
									],
									"adjust_pure_negative" : True,
									"boost" : 1.0
									}
								},
								"_source" : False,
								"sort" : [
									{
									"_doc" : {
										"order" : "asc"
									}
									}
								],
								"track_total_hits" : 2147483647
								}
						search_df=es.search(index=index_name,body=count_query,request_timeout=120)
						count=search_df['hits']['total']['value']
						query={
								"size" : count,
								"query" : {
									"bool" : {
									"must" : [
										{
										"term" : {
											"storename.keyword" : {
											"value" : store_name,
											"boost" : 1.0
											}
										}
										},
										{
										"range" : {
											"slip_date" : {
											"from" : from_date,
											"to" : to_date,
											"include_lower" : True,
											"include_upper" : True,
											"time_zone" : "Z",
											"boost" : 1.0,
											"format":"yyyy-MM-dd"
											}
										}
										}
									],
									"adjust_pure_negative" : True,
									"boost" : 1.0
									}
								},
								"_source" : False,
								"fields" : [
									{
									"field" : "veh_regn_no"
									},
									{
									"field" : "slip_no"
									},
									{
									"field" : "slip_date",
									"format" : "yyyy-MM-dd"
									},
									{
									"field" : "leci_no"
									},
									{
									"field" : "quality_ind"
									},
									{
									"field" : "storename"
									},
									{
									"field" : "dockname"
									},
									{
									"field" : "vs_dockcheckintime"
									},
									{
									"field" : "vs_dockcheckouttime"
									},
									{
									"field" : "vehicle_status"
									},
									{
									"field" : "vs_out_in_hrs"
									}
								],
								"sort" : [
									{
									"_doc" : {
										"order" : "asc"
									}
									}
								]
								}
					else:
						count_query={
								"size" : 0,
								"query" : {
									"bool" : {
									"must" : [
										{
										"range" : {
											"slip_date" : {
											"from" : from_date,
											"to" : to_date,
											"include_lower" : True,
											"include_upper" : True,
											"time_zone" : "Z",
											"boost" : 1.0,
											"format":"yyyy-MM-dd"
											}
										}
										}
									],
									"adjust_pure_negative" : True,
									"boost" : 1.0
									}
								},
								"_source" : False,
								"sort" : [
									{
									"_doc" : {
										"order" : "asc"
									}
									}
								],
								"track_total_hits" : 2147483647
								}
						search_df=es.search(index=index_name,body=count_query,request_timeout=120)
						count=search_df['hits']['total']['value']
						query={
								"size" : count,
								"query" : {
									"range" : {
									"slip_date" : {
										"from" : from_date,
										"to" : to_date,
										"include_lower" : True,
										"include_upper" : True,
										"time_zone" : "Z",
										"boost" : 1.0,
										"format":"yyyy-MM-dd"
									}
									}
								},
								"_source" : False,
								"fields" : [
									{
									"field" : "veh_regn_no"
									},
									{
									"field" : "slip_no"
									},
									{
									"field" : "slip_date",
									"format" : "yyyy-MM-dd"
									},
									{
									"field" : "leci_no"
									},
									{
									"field" : "quality_ind"
									},
									{
									"field" : "storename"
									},
									{
									"field" : "dockname"
									},
									{
									"field" : "vs_dockcheckintime"
									},
									{
									"field" : "vs_dockcheckouttime"
									},
									{
									"field" : "vehicle_status"
									},
									{
									"field" : "vs_out_in_hrs"
									}
								],
								"sort" : [
									{
									"_doc" : {
										"order" : "asc"
									}
									}
								]
								}
					res_vin=es.search(index=index_name,body=query,size=count,request_timeout=120)
					dict={}
					list=[]
					x2=res_vin['hits'].get('hits')
					for i in x2:
					    del i['_index']
					    del i['_type']
					    del i['_id']
					    del i['_score']
					    dict=i['fields']
					    list.append(dict.copy())
					'''for i in list:
						#i['vs_out_in_hrs']=round(i['vs_out_in_hrs']*60,2)
						i['vs_dockcheckintime']=i['vs_dockcheckintime'].replace("T"," ")
						i['vs_dockcheckouttime']=i['vs_dockcheckouttime'].replace("T"," ")'''
					return list
	#detailed view export csv
	def detailedviewexport(self,dock_name,store_name,from_date,to_date):
					from elasticsearch import Elasticsearch, helpers
					import requests
					#es = Elasticsearch(["http://elastic:elastic@172.31.154.203:9200/"])
					#es = Elasticsearch(["http://elastic:elastic@172.31.158.159:9200/"])
					if dock_name!='undefined' and store_name!='undefined':
						count_query={
								"size" : 0,
								"query" : {
									"bool" : {
									"must" : [
										{
										"bool" : {
											"must" : [
											{
												"term" : {
												"dockid" : {
													"value" : dock_name,
													"boost" : 1.0
												}
												}
											},
											{
												"term" : {
												"storename.keyword" : {
													"value" : store_name,
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
											"slip_date" : {
											"from" : from_date,
											"to" : to_date,
											"include_lower" : True,
											"include_upper" : True,
											"time_zone" : "Z",
											"boost" : 1.0,
											"format":"yyyy-MM-dd"
											}
										}
										}
									],
									"adjust_pure_negative" : True,
									"boost" : 1.0
									}
								},
								"_source" : False,
								"sort" : [
									{
									"_doc" : {
										"order" : "asc"
									}
									}
								],
								"track_total_hits" : 2147483647
								}
						search_df=es.search(index=index_name,body=count_query,request_timeout=120)
						count=search_df['hits']['total']['value']
						query={
							"size" : count,
							"query" : {
								"bool" : {
								"must" : [
									{
									"bool" : {
										"must" : [
										{
											"term" : {
											"dockid" : {
												"value" : dock_name,
												"boost" : 1.0
											}
											}
										},
										{
											"term" : {
											"storename.keyword" : {
												"value" : store_name,
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
										"slip_date" : {
										"from" : from_date,
										"to" : to_date,
										"include_lower" : True,
										"include_upper" : True,
										"time_zone" : "Z",
										"boost" : 1.0,
										"format":"yyyy-MM-dd"
										}
									}
									}
								],
								"adjust_pure_negative" : True,
								"boost" : 1.0
								}
							},
							"_source" : {
								"includes" : [
									"veh_regn_no",
									 "slip_no",
									"leci_no",
									"quality_ind",
									"storename",
									"dockname",
									"vs_dockcheckintime",
									"vs_dockcheckouttime",
									"slip_date",
									"vehicle_status",
									"vs_out_in_hrs"
								],
								"excludes" : [ ]
								},
							"docvalue_fields" : [
									{
									"field" : "slip_date",
									"format" : "yyyy-MM-dd"
									}
									],
							"sort" : [
								{
								"_doc" : {
									"order" : "asc"
								}
								}
							]
							}
					elif dock_name != "undefined":
						count_query={
								"size" : 0,
								"query" : {
									"bool" : {
									"must" : [
										{
										"bool" : {
											"must" : [
											{
												"term" : {
												"dockid" : {
													"value" : dock_name,
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
											"slip_date" : {
											"from" : from_date,
											"to" : to_date,
											"include_lower" : True,
											"include_upper" : True,
											"time_zone" : "Z",
											"boost" : 1.0,
											"format":"yyyy-MM-dd"
											}
										}
										}
									],
									"adjust_pure_negative" : True,
									"boost" : 1.0
									}
								},
								"_source" : False,
								"sort" : [
									{
									"_doc" : {
										"order" : "asc"
									}
									}
								],
								"track_total_hits" : 2147483647
								}
						search_df=es.search(index=index_name,body=count_query,request_timeout=120)
						count=search_df['hits']['total']['value']
						query={
								"size" : count,
								"query" : {
									"bool" : {
									"must" : [
										{
										"term" : {
											"dockid" : {
											"value" : dock_name,
											"boost" : 1.0
											}
										}
										},
										{
										"range" : {
											"slip_date" : {
											"from" : from_date,
											"to" : to_date,
											"include_lower" : True,
											"include_upper" : True,
											"time_zone" : "Z",
											"boost" : 1.0,
											"format":"yyyy-MM-dd"
											}
										}
										}
									],
									"adjust_pure_negative" : True,
									"boost" : 1.0
									}
								},
								"_source" : {
								"includes" : [
									"veh_regn_no",
									 "slip_no",
									"leci_no",
									"quality_ind",
									"storename",
									"dockname",
									"vs_dockcheckintime",
									"vs_dockcheckouttime",
									"slip_date",
									"vehicle_status",
									"vs_out_in_hrs"
								],
								"excludes" : [ ]
								},
							"docvalue_fields" : [
									{
									"field" : "slip_date",
									"format" : "yyyy-MM-dd"
									}
									],
								"sort" : [
									{
									"_doc" : {
										"order" : "asc"
									}
									}
								]
								}
					elif store_name!= "undefined":
						count_query={
								"size" : 0,
								"query" : {
									"bool" : {
									"must" : [
										{
										"bool" : {
											"must" : [
											{
												"term" : {
												"storename.keyword" : {
													"value" : store_name,
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
											"slip_date" : {
											"from" : from_date,
											"to" : to_date,
											"include_lower" : True,
											"include_upper" : True,
											"time_zone" : "Z",
											"boost" : 1.0,
											"format":"yyyy-MM-dd"
											}
										}
										}
									],
									"adjust_pure_negative" : True,
									"boost" : 1.0
									}
								},
								"_source" : False,
								"sort" : [
									{
									"_doc" : {
										"order" : "asc"
									}
									}
								],
								"track_total_hits" : 2147483647
								}
						search_df=es.search(index=index_name,body=count_query,request_timeout=120)
						count=search_df['hits']['total']['value']
						query={
								"size" : count,
								"query" : {
									"bool" : {
									"must" : [
										{
										"term" : {
											"storename.keyword" : {
											"value" : store_name,
											"boost" : 1.0
											}
										}
										},
										{
										"range" : {
											"slip_date" : {
											"from" : from_date,
											"to" : to_date,
											"include_lower" : True,
											"include_upper" : True,
											"time_zone" : "Z",
											"boost" : 1.0,
											"format":"yyyy-MM-dd"
											}
										}
										}
									],
									"adjust_pure_negative" : True,
									"boost" : 1.0
									}
								},
								"_source" : {
								"includes" : [
									"veh_regn_no",
									 "slip_no",
									"leci_no",
									"quality_ind",
									"storename",
									"dockname",
									"vs_dockcheckintime",
									"vs_dockcheckouttime",
									"slip_date",
									"vehicle_status",
									"vs_out_in_hrs"
								],
								"excludes" : [ ]
								},
							"docvalue_fields" : [
									{
									"field" : "slip_date",
									"format" : "yyyy-MM-dd"
									}
									],
								"sort" : [
									{
									"_doc" : {
										"order" : "asc"
									}
									}
								]
								}
					else:
						count_query={
								"size" : 0,
								"query" : {
									"bool" : {
									"must" : [
										{
										"range" : {
											"slip_date" : {
											"from" : from_date,
											"to" : to_date,
											"include_lower" : True,
											"include_upper" : True,
											"time_zone" : "Z",
											"boost" : 1.0,
											"format":"yyyy-MM-dd"
											}
										}
										}
									],
									"adjust_pure_negative" : True,
									"boost" : 1.0
									}
								},
								"_source" : False,
								"sort" : [
									{
									"_doc" : {
										"order" : "asc"
									}
									}
								],
								"track_total_hits" : 2147483647
								}
						search_df=es.search(index=index_name,body=count_query,request_timeout=120)
						count=search_df['hits']['total']['value']
						query={
								"size" : count,
								"query" : {
									"range" : {
									"slip_date" : {
										"from" : from_date,
										"to" : to_date,
										"include_lower" : True,
										"include_upper" : True,
										"time_zone" : "Z",
										"boost" : 1.0,
										"format":"yyyy-MM-dd"
									}
									}
								},
								"_source" : {
								"includes" : [
									"veh_regn_no",
									 "slip_no",
									"leci_no",
									"quality_ind",
									"storename",
									"dockname",
									"vs_dockcheckintime",
									"vs_dockcheckouttime",
									"slip_date",
									"vehicle_status",
									"vs_out_in_hrs"
								],
								"excludes" : [ ]
								},
							"docvalue_fields" : [
									{
									"field" : "slip_date",
									"format" : "yyyy-MM-dd"
									}
									],
								"sort" : [
									{
									"_doc" : {
										"order" : "asc"
									}
									}
								]
								}
					res_vin=es.search(index=index_name,body=query,size=count,request_timeout=120)
					dict={}
					list=[]
					x2=res_vin['hits'].get('hits')
					for i in x2:
						del i['_index']
						del i['_type']
						del i['_id']
						del i['_score']
						dict=i['_source']
						list.append(dict.copy())
					for i in list:
						i['vs_out_in_hrs']=round(i['vs_out_in_hrs']*60,2)
						i['vs_dockcheckintime']=i['vs_dockcheckintime'].replace("T"," ")
						i['vs_dockcheckouttime']=i['vs_dockcheckouttime'].replace("T"," ")
						if i['vehicle_status']=='1':
							i['vehicle_status']='Leci Created'
						elif i['vehicle_status']=='2':
							i['vehicle_status']='Security Scan'
						elif i['vehicle_status']=='3':
							i['vehicle_status']='Dock Checkin'
						elif i['vehicle_status']=='4':
							i['vehicle_status']='Dock Checkout'
					return list
	#Graph 1:Overall slip gendrated trend
	def overall_slip_trend(self,dock_name,store_name,from_date,to_date):
					from elasticsearch import Elasticsearch, helpers
					import requests
					#es = Elasticsearch(["http://elastic:elastic@172.31.154.203:9200/"])
					#es = Elasticsearch(["http://elastic:elastic@172.31.158.159:9200/"])
					if dock_name!='undefined' and store_name!='undefined':
						query={
						"size" : 0,
						"query" : {
							"bool" : {
							"must" : [
								{
								"bool" : {
									"must" : [
									{
										"term" : {
										"dockid" : {
											"value" : dock_name,
											"boost" : 1.0
										}
										}
									},
									{
										"term" : {
										"storename.keyword" : {
											"value" : store_name,
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
									"slip_date" : {
									"from" : from_date,
									"to" : to_date,
									"include_lower" : True,
									"include_upper" : True,
									"time_zone" : "Z",
									"boost" : 1.0,
									"format":"yyyy-MM-dd"
									}
								}
								}
							],
							"adjust_pure_negative" : True,
							"boost" : 1.0
							}
						},
						"_source" : False,
						"aggregations" : {
							"groupby" : {
							"composite" : {
								"size" : 1000,
								"sources" : [
								{
									"slip_date" : {
									"terms" : {
										"field" : "slip_date",
										"missing_bucket" : True,
										"order" : "asc"
									}
									}
								}
								]
							},
							"aggregations" : {
								"slip_no" : {
								"filter" : {
									"exists" : {
									"field" : "slip_no",
									"boost" : 1.0
									}
								}
								}
							}
							}
						}
						}
					elif dock_name != "undefined":
						query={
							"size" : 0,
							"query" : {
								"bool" : {
								"must" : [
									{
									"term" : {
										"dockid" : {
										"value" : dock_name,
										"boost" : 1.0
										}
									}
									},
									{
									"range" : {
										"slip_date" : {
										"from" : from_date,
										"to" : to_date,
										"include_lower" : True,
										"include_upper" : True,
										"time_zone" : "Z",
										"boost" : 1.0,
										"format":"yyyy-MM-dd"
										}
									}
									}
								],
								"adjust_pure_negative" : True,
								"boost" : 1.0
								}
							},
							"_source" : False,
							"aggregations" : {
								"groupby" : {
								"composite" : {
									"size" : 1000,
									"sources" : [
									{
										"slip_date" : {
										"terms" : {
											"field" : "slip_date",
											"missing_bucket" : True,
											"order" : "asc"
										}
										}
									}
									]
								},
								"aggregations" : {
									"slip_no" : {
									"filter" : {
										"exists" : {
										"field" : "slip_no",
										"boost" : 1.0
										}
									}
									}
								}
								}
							}
							}
					elif store_name!= "undefined":
						query={
							"size" : 0,
							"query" : {
								"bool" : {
								"must" : [
									{
									"term" : {
										"storename.keyword" : {
										"value" : store_name,
										"boost" : 1.0
										}
									}
									},
									{
									"range" : {
										"slip_date" : {
										"from" : from_date,
										"to" : to_date,
										"include_lower" : True,
										"include_upper" : True,
										"time_zone" : "Z",
										"boost" : 1.0,
										"format":"yyyy-MM-dd"
										}
									}
									}
								],
								"adjust_pure_negative" : True,
								"boost" : 1.0
								}
							},
							"_source" : False,
							"aggregations" : {
								"groupby" : {
								"composite" : {
									"size" : 1000,
									"sources" : [
									{
										"slip_date" : {
										"terms" : {
											"field" : "slip_date",
											"missing_bucket" : True,
											"order" : "asc"
										}
										}
									}
									]
								},
								"aggregations" : {
									"slip_no" : {
									"filter" : {
										"exists" : {
										"field" : "slip_no",
										"boost" : 1.0
										}
									}
									}
								}
								}
							}
							}
					else:
						query={
						"size" : 0,
						"query" : {
							"range" : {
							"slip_date" : {
								"from" : from_date,
								"to" : to_date,
								"include_lower" : True,
								"include_upper" : True,
								"time_zone" : "Z",
								"boost" : 1.0,
								"format":"yyyy-MM-dd"
							}
							}
						},
						"_source" : False,
						"aggregations" : {
							"groupby" : {
							"composite" : {
								"size" : 1000,
								"sources" : [
								{
									"slip_date" : {
									"terms" : {
										"field" : "slip_date",
										"missing_bucket" : True,
										"order" : "asc"
									}
									}
								}
								]
							},
							"aggregations" : {
								"slip_no" : {
								"filter" : {
									"exists" : {
									"field" : "slip_no",
									"boost" : 1.0
									}
								}
								}
							}
							}
						}
						}
					res_vin=es.search(index=index_name,body=query,size=10000,request_timeout=120)
					slip_dict={}
					count_dict={}
					store_list=[]
					x2=res_vin['aggregations'].get('groupby').get('buckets')
					for i in x2:
						data={}
						data['x'] = datetime.fromtimestamp(int(str(i['key']['slip_date'])[0:10])).strftime('%Y-%m-%d')
						data['y']=i['slip_no']['doc_count']
						#data['text']=i['slip_no']['doc_count']
						data['texttemplate']="<b>%{y}</b>"
						data['textfont']="{'size':'10'}"
						store_list.append(data)
					newdict={}
					for k,v in [(key,d[key]) for d in store_list for key in d]:
						if k not in newdict:
							newdict[k]=[v]
						else: 
							newdict[k].append(v)
					if len(x2)>0:
						newdict['type']='scatter'
						newdict['mode']='lines+markers+text'
						newdict['textposition']='top center left'
						newdict['marker']= {'color': '#eebcde'}
					#temp={}
					#temp[0]=newdict
					return newdict
	#Graph 2:Top 5 vehicle turn around time
	def vehicle_turn_around_time(self,dock_name,store_name,from_date,to_date):
					from elasticsearch import Elasticsearch, helpers
					import requests
					#es = Elasticsearch(["http://elastic:elastic@172.31.154.203:9200/"])
					#es = Elasticsearch(["http://elastic:elastic@172.31.158.159:9200/"])
					if dock_name!='undefined' and store_name!='undefined':
						query={
							"size" : 0,
							"query" : {
								"bool" : {
								"must" : [
									{
									"bool" : {
										"must" : [
										{
											"term" : {
											"dockid" : {
												"value" : dock_name,
												"boost" : 1.0
											}
											}
										},
										{
											"term" : {
											"storename.keyword" : {
												"value" : store_name,
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
										"slip_date" : {
										"from" : from_date,
										"to" : to_date,
										"include_lower" : True,
										"include_upper" : True,
										"time_zone" : "Z",
										"boost" : 1.0
										}
									}
									}
								],
								"adjust_pure_negative" : True,
								"boost" : 1.0
								}
							},
							"_source" : False,
							"aggregations" : {
								"groupby" : {
								"composite" : {
									"size" : 1000,
									"sources" : [
									{
										"veh_regn_no" : {
										"terms" : {
											"field" : "veh_regn_no.keyword",
											"missing_bucket" : True,
											"order" : "asc"
										}
										}
									}
									]
								},
								"aggregations" : {
									"vs_out_slip_hrs" : {
									"sum" : {
										"field" : "vs_out_slip_hrs"
									}
									},
									"bucket_sort":{
									"bucket_sort": {
										"sort": [
										{ "vs_out_slip_hrs": { "order": "desc"}
										}
										],
										"size": 5
									}
									}
								}
								}
							}
							}
					elif dock_name != "undefined":
						query={
							"size" : 0,
							"query" : {
								"bool" : {
								"must" : [
									{
									"term" : {
										"dockid" : {
										"value" : dock_name,
										"boost" : 1.0
										}
									}
									},
									{
									"range" : {
										"slip_date" : {
										"from" : from_date,
										"to" : to_date,
										"include_lower" : True,
										"include_upper" : True,
										"time_zone" : "Z",
										"boost" : 1.0
										}
									}
									}
								],
								"adjust_pure_negative" : True,
								"boost" : 1.0
								}
							},
							"_source" : False,
							"aggregations" : {
								"groupby" : {
								"composite" : {
									"size" : 1000,
									"sources" : [
									{
										"veh_regn_no" : {
										"terms" : {
											"field" : "veh_regn_no.keyword",
											"missing_bucket" : True,
											"order" : "asc"
										}
										}
									}
									]
								},
								"aggregations" : {
									"vs_out_slip_hrs" : {
									"sum" : {
										"field" : "vs_out_slip_hrs"
									}
									},
									"bucket_sort":{
									"bucket_sort": {
										"sort": [
										{ "vs_out_slip_hrs": { "order": "desc"}
										}
										],
										"size": 5
									}
									}
								}
								}
							}
							}
					elif store_name!= "undefined":
						query={
							"size" : 0,
							"query" : {
								"bool" : {
								"must" : [
									{
									"term" : {
										"storename.keyword" : {
										"value" : store_name,
										"boost" : 1.0
										}
									}
									},
									{
									"range" : {
										"slip_date" : {
										"from" : from_date,
										"to" : to_date,
										"include_lower" : True,
										"include_upper" : True,
										"time_zone" : "Z",
										"boost" : 1.0
										}
									}
									}
								],
								"adjust_pure_negative" : True,
								"boost" : 1.0
								}
							},
							"_source" : False,
							"aggregations" : {
								"groupby" : {
								"composite" : {
									"size" : 1000,
									"sources" : [
									{
										"veh_regn_no" : {
										"terms" : {
											"field" : "veh_regn_no.keyword",
											"missing_bucket" : True,
											"order" : "asc"
										}
										}
									}
									]
								},
								"aggregations" : {
									"vs_out_slip_hrs" : {
									"sum" : {
										"field" : "vs_out_slip_hrs"
									}
									},
									"bucket_sort":{
									"bucket_sort": {
										"sort": [
										{ "vs_out_slip_hrs": { "order": "desc"}
										}
										],
										"size": 5
									}
									}
								}
								}
							}
							}
					else:
						query={
							"size" : 0,
							"query" : {
								"range" : {
								"slip_date" : {
									"from" : from_date,
									"to" : to_date,
									"include_lower" : True,
									"include_upper" : True,
									"time_zone" : "Z",
									"boost" : 1.0
								}
								}
							},
							"_source" : False,
							"aggregations" : {
								"groupby" : {
								"composite" : {
									"size" : 1000,
									"sources" : [
									{
										"veh_regn_no" : {
										"terms" : {
											"field" : "veh_regn_no.keyword",
											"missing_bucket" : True,
														"order" : "asc"
													}
													}
												}
												]
											},
											"aggregations" : {
												"vs_out_slip_hrs" : {
												"sum" : {
													"field" : "vs_out_slip_hrs"
												}
												},
												"bucket_sort":{
												"bucket_sort": {
													"sort": [
													{ "vs_out_slip_hrs": { "order": "desc"}
													}
													],
													"size": 5
												}
												}
											}
											}
										}
										}
					res_vin=es.search(index=index_name,body=query,size=10000,request_timeout=120)
					x2=res_vin['aggregations'].get('groupby').get('buckets')
					count_dict={}
					list=[]
					for i in x2:
						data={}
						data['x']=i['key']['veh_regn_no']
						data['y']=round(i['vs_out_slip_hrs']['value'],2)
						#data['text']=round(i['vs_out_slip_hrs']['value'],2)
						data['texttemplate'] ="<b>%{value}</b>"
						data['textfont']="{'size':'10'}"
						list.append(data)
					newdict={}
					for k,v in [(key,d[key]) for d in list for key in d]:
						if k not in newdict:
							newdict[k]=[v]
						else: 
							newdict[k].append(v)
					if len(x2)>0:
						newdict['type']='bar'
						newdict['mode']='bar+markers+text'
						newdict['textposition']='top center left'
						newdict['marker']= {'color': 'rgb(255, 99, 132)'}
					return newdict
	#Graph:3 Top 5 Congested Docks
	def congested_docks(self,dock_name,store_name,from_date,to_date):
					from elasticsearch import Elasticsearch, helpers
					import requests
					#es = Elasticsearch(["http://elastic:elastic@172.31.154.203:9200/"])
					#es = Elasticsearch(["http://elastic:elastic@172.31.158.159:9200/"])
					if dock_name!='undefined' and store_name!='undefined':
						query={
						"size" : 0,
						"query" : {
							"bool" : {
							"must" : [
								{
								"bool" : {
									"must" : [
									{
										"exists" : {
										"field" : "dockid",
										"boost" : 1.0
										}
									},
									{
										"term" : {
										"dockid" : {
											"value" : dock_name,
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
									"must" : [
									{
										"term" : {
										"storename.keyword" : {
											"value" : store_name,
											"boost" : 1.0
										}
										}
									},
									{
										"range" : {
										"slip_date" : {
											"from" : from_date,
											"to" : to_date,
											"include_lower" : True,
											"include_upper" : True,
											"time_zone" : "Z",
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
						"_source" : False,
						"aggregations" : {
							"groupby" : {
							"composite" : {
								"size" : 1000,
								"sources" : [
								{
									"dockname" : {
									"terms" : {
										"field" : "dockname.keyword",
										"missing_bucket" : True,
										"order" : "asc"
									}
									}
								}
								]
							},
							"aggregations" : {
								"veh_regn_no" : {
								"filter" : {
									"exists" : {
									"field" : "veh_regn_no",
									"boost" : 1.0
									}
								}
								},
								"bucket_sort":{
								"bucket_sort": {
									"sort": [
									{ "_count": { "order": "desc"}
									}
									],
									"size": 5
								}
								}
							}
							}
						}
						}
					elif dock_name != "undefined":
						query={
						"size" : 0,
						"query" : {
							"bool" : {
							"must" : [
								{
								"bool" : {
									"must" : [
									{
										"exists" : {
										"field" : "dockid",
										"boost" : 1.0
										}
									},
									{
										"term" : {
										"dockid" : {
											"value" : dock_name,
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
									"slip_date" : {
									"from" : from_date,
									"to" : to_date,
									"include_lower" : True,
									"include_upper" : True,
									"time_zone" : "Z",
									"boost" : 1.0
									}
								}
								}
							],
							"adjust_pure_negative" : True,
							"boost" : 1.0
							}
						},
						"_source" : False,
						"aggregations" : {
							"groupby" : {
							"composite" : {
								"size" : 1000,
								"sources" : [
								{
									"dockname" : {
									"terms" : {
										"field" : "dockname.keyword",
										"missing_bucket" : True,
										"order" : "asc"
									}
									}
								}
								]
							},
							"aggregations" : {
								"veh_regn_no" : {
								"filter" : {
									"exists" : {
									"field" : "veh_regn_no",
									"boost" : 1.0
									}
								}
								},
								"bucket_sort":{
								"bucket_sort": {
									"sort": [
									{ "_count": { "order": "desc"}
									}
									],
									"size": 5
								}
								}
							}
							}
						}
						}
					elif store_name!= "undefined":
						query={
						"size" : 0,
						"query" : {
							"bool" : {
							"must" : [
								{
								"bool" : {
									"must" : [
									{
										"exists" : {
										"field" : "dockid",
										"boost" : 1.0
										}
									},
									{
										"term" : {
										"storename.keyword" : {
											"value" : store_name,
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
									"slip_date" : {
									"from" : from_date,
									"to" : to_date,
									"include_lower" : True,
									"include_upper" : True,
									"time_zone" : "Z",
									"boost" : 1.0
									}
								}
								}
							],
							"adjust_pure_negative" : True,
							"boost" : 1.0
							}
						},
						"_source" : False,
						"aggregations" : {
							"groupby" : {
							"composite" : {
								"size" : 1000,
								"sources" : [
								{
									"dockname" : {
									"terms" : {
										"field" : "dockname.keyword",
										"missing_bucket" : True,
										"order" : "asc"
									}
									}
								}
								]
							},
							"aggregations" : {
								"veh_regn_no" : {
								"filter" : {
									"exists" : {
									"field" : "veh_regn_no",
									"boost" : 1.0
									}
								}
								},
								"bucket_sort":{
								"bucket_sort": {
									"sort": [
									{ "_count": { "order": "desc"}
									}
									],
									"size": 5
								}
								}
							}
							}
						}
						}
					else:
						query={
						"size" : 0,
					"query" : {
						"bool" : {
						"must" : [
							{
							"bool" : {
								"must" : [
								{
									"bool" : {
									"must_not" : [
										{
										"term" : {
											"dockname.keyword" : {
											"value" : "",
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
									"must_not" : [
										{
										"term" : {
											"dockid" : {
											"value" : 0,
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
							{
							"range" : {
								"slip_date" : {
								"from" : from_date,
								"to" : to_date,
								"include_lower" : True,
								"include_upper" : True,
								"time_zone" : "Z",
								"boost" : 1.0
								}
							}
							}
						],
						"adjust_pure_negative" : True,
						"boost" : 1.0
						}
					},
						"_source" : False,
						"aggregations" : {
							"groupby" : {
							"composite" : {
								"size" : 1000,
								"sources" : [
								{
									"dockname" : {
									"terms" : {
										"field" : "dockname.keyword",
										"missing_bucket" : True,
										"order" : "asc"
									}
									}
								}
								]
							},
							"aggregations" : {
								"veh_regn_no" : {
								"filter" : {
									"exists" : {
									"field" : "veh_regn_no",
									"boost" : 1.0
									}
								}
								},
								"bucket_sort":{
								"bucket_sort": {
									"sort": [
									{ "_count": { "order": "desc"}
									}
									],
									"size": 5
								}
								}
							}
							}
						}
						}
					res_vin=es.search(index=index_name,body=query,size=10000,request_timeout=120)
					x2=res_vin['aggregations'].get('groupby').get('buckets')
					list=[]
					for i in x2:
						data={}
						data['x']=i['key']['dockname']
						#data['x']='dock_'+str(i['key']['dockname'])
						data['y']=i['veh_regn_no']['doc_count']
						#data['text']=i['veh_regn_no']['doc_count']
						data['texttemplate']="<b>%{y}</b>"
						data['textfont']="{'size':'10'}"
						list.append(data)
					newdict={}
					for k,v in [(key,d[key]) for d in list for key in d]:
						if k not in newdict:
							newdict[k]=[v]
						else: 
							newdict[k].append(v)
					if len(x2)>0:
						newdict['type']='scatter'
						newdict['mode']='lines+markers+text'
						newdict['textposition']='top center left'
						newdict['marker']= {'color': 'rgb(83, 262, 335)'}
					return newdict
	#Graph 4:Dock Wise Vehicle Turn around time
	def dock_vehicle_turnaround_time(self,dock_name,store_name,from_date,to_date):
					from elasticsearch import Elasticsearch, helpers
					import requests
					#es = Elasticsearch(["http://elastic:elastic@172.31.154.203:9200/"])
					#es = Elasticsearch(["http://elastic:elastic@172.31.158.159:9200/"])
					if dock_name!='undefined' and store_name!='undefined':
						query={
							"size" : 0,
							"query" : {
								"bool" : {
								"must" : [
									{
									"bool" : {
										"must" : [
										{
											"term" : {
											"dockid" : {
												"value" : dock_name,
												"boost" : 1.0
											}
											}
										},
										{
											"term" : {
											"storename.keyword" : {
												"value" : store_name,
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
										"slip_date" : {
										"from" : from_date,
										"to" : to_date,
										"include_lower" : True,
										"include_upper" : True,
										"time_zone" : "Z",
										"boost" : 1.0
										}
									}
									}
								],
								"adjust_pure_negative" : True,
								"boost" : 1.0
								}
							},
							"_source" : False,
							"aggregations" : {
								"groupby" : {
								"composite" : {
									"size" : 1000,
									"sources" : [
									{
										"dockname" : {
										"terms" : {
											"field" : "dockname.keyword",
											"missing_bucket" : True,
											"order" : "asc"
										}
										}
									}
									]
								},
								"aggregations" : {
									"vs_out_in_hrs" : {
									"sum" : {
										"field" : "vs_out_in_hrs"
									}
									}
								}
								}
							}
							}
					elif dock_name != "undefined":
						query={
						"size" : 0,
						"query" : {
							"bool" : {
							"must" : [
								{
								"term" : {
									"dockid" : {
									"value" : dock_name,
									"boost" : 1.0
									}
								}
								},
								{
								"range" : {
									"slip_date" : {
									"from" : from_date,
									"to" : to_date,
									"include_lower" : True,
									"include_upper" : True,
									"time_zone" : "Z",
									"boost" : 1.0
									}
								}
								}
							],
							"adjust_pure_negative" : True,
							"boost" : 1.0
							}
						},
						"_source" : False,
						"aggregations" : {
							"groupby" : {
							"composite" : {
								"size" : 1000,
								"sources" : [
								{
									"dockname" : {
									"terms" : {
										"field" : "dockname.keyword",
										"missing_bucket" : True,
										"order" : "asc"
									}
									}
								}
								]
							},
							"aggregations" : {
								"vs_out_in_hrs" : {
								"sum" : {
									"field" : "vs_out_in_hrs"
								}
								}
							}
							}
						}
						}
					elif store_name!= "undefined":
						query={
							"size" : 0,
							"query" : {
								"bool" : {
								"must" : [
									{
									"term" : {
										"storename.keyword" : {
										"value" : store_name,
										"boost" : 1.0
										}
									}
									},
									{
									"range" : {
										"slip_date" : {
										"from" : from_date,
										"to" : to_date,
										"include_lower" : True,
										"include_upper" : True,
										"time_zone" : "Z",
										"boost" : 1.0
										}
									}
									}
								],
								"adjust_pure_negative" : True,
								"boost" : 1.0
								}
							},
							"_source" : False,
							"aggregations" : {
								"groupby" : {
								"composite" : {
									"size" : 1000,
									"sources" : [
									{
										"dockname" : {
										"terms" : {
											"field" : "dockname.keyword",
											"missing_bucket" : True,
											"order" : "asc"
										}
										}
									}
									]
								},
								"aggregations" : {
									"vs_out_in_hrs" : {
									"sum" : {
										"field" : "vs_out_in_hrs"
									}
									}
								}
								}
							}
							}
					else:
						query={
							"size" : 0,
								"query" : {
									"bool" : {
									"must" : [
										{
										"bool" : {
											"must" : [
											{
												"bool" : {
												"must_not" : [
													{
													"term" : {
														"dockname.keyword" : {
														"value" : "",
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
												"must_not" : [
													{
													"term" : {
														"dockid" : {
														"value" : 0,
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
										{
										"range" : {
											"slip_date" : {
											"from" : from_date,
											"to" : to_date,
											"include_lower" : True,
											"include_upper" : True,
											"time_zone" : "Z",
											"boost" : 1.0
											}
										}
										}
									],
									"adjust_pure_negative" : True,
									"boost" : 1.0
									}
								},
							"_source" : False,
							"aggregations" : {
								"groupby" : {
								"composite" : {
									"size" : 1000,
									"sources" : [
									{
										"dockname" : {
										"terms" : {
											"field" : "dockname.keyword",
											"missing_bucket" : True,
											"order" : "asc"
										}
										}
									}
									]
								},
								"aggregations" : {
									"vs_out_in_hrs" : {
									"sum" : {
										"field" : "vs_out_in_hrs"
									}
									}
								}
								}
							}
							}
					res_vin=es.search(index=index_name,body=query,size=10000,request_timeout=120)
					store_list=[]
					x2=res_vin['aggregations'].get('groupby').get('buckets')
					#data={}
					for i in x2:
						data={}
						#if i['vs_out_in_hrs']['value'] >0:
						data['x']=i['key']['dockname']
						#data['x']='dock_'+str(i['key']['dockname'])
						data['y']=round(i['vs_out_in_hrs']['value'],2)
						data['texttemplate'] ="<b>%{value}</b>"
						data['textfont']="{'size':'10'}"
						store_list.append(data)
					newdict={}
					for k,v in [(key,d[key]) for d in store_list for key in d]:
						if k not in newdict:
							newdict[k]=[v]
						else: 
							newdict[k].append(v)
					if len(x2)>0:
						newdict['type']='bar'
						newdict['mode']='bar+markers+text'
						newdict['textposition']='top center left'
						newdict['marker']= {'color': 'rgb(53, 162, 235)'}
					return newdict
