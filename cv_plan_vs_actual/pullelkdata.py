from django.test import TestCase

# Create your tests here.
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from datetime import date
from elasticsearch import Elasticsearch, helpers
import requests
#from datetime import datetime

from datetime import date

import pandas as pd

from datetime import datetime, timedelta
from collections import ChainMap
from datetime import datetime
#import datetime

from pandasql import sqldf

import pytz
timezone = pytz.timezone('GMT')
today = date.today()
today=str(today)

import sys
sys.path.append("cv_api")
import cv_elk_indices
import db_connection

db_obj=db_connection.db_con()
es=db_obj.elk()

index_obj=cv_elk_indices.elk_indices()
index_name=index_obj.plan_vs_actual()
index_dropdown=index_obj.plan_vs_actual_digital()
index_vc=index_obj.plan_vs_actual_vc_description()
index_status=index_obj.plan_vs_actual_status()

#def index_name(self,from_date,to_date):
#	index_name="flat_mfg_produced_material_v1_kafka"
#	return index_name


def hr_month_yr(self,duration):
	#daily wise
	if duration=='daily':
		duration={
				"date_histogram" : {
					"field" : "modifiedAt",
						"fixed_interval": "1d"
						}
						}
	#hour wise
	elif duration=='hourly':
		duration={
				"date_histogram" : {
					"field" : "modifiedAt",
						"fixed_interval": "15m"
						}
						}
	#month wise trend:
	elif duration=='monthly':
		duration={
					"terms" : {
					"script" : {
					"source" : "InternalSqlScriptUtils.format(InternalQlScriptUtils.docValue(doc,params.v0),params.v1,params.v2)",
					"lang" : "painless",
					"params" : {
					"v0" : "modifiedAt",
					"v1" : "yyyy-MM",
					"v2" : "Z"
					}
					},
					"missing_bucket" : True,
					"value_type" : "string",
					"order" : "asc"
					}
					}
	# yearly wise trend:
	elif duration=='yearly':
		duration={
					"terms" : {
					"script" : {
					"source" : "InternalSqlScriptUtils.format(InternalQlScriptUtils.docValue(doc,params.v0),params.v1,params.v2)",
					"lang" : "painless",
					"params" : {
					"v0" : "modifiedAt",
					"v1" : "yyyy",
					"v2" : "Z"
					}
					},
					"missing_bucket" : True,
					"value_type" : "string",
					"order" : "asc"
					}
					}
	return duration

def requried_hr_month_yr(self,req_duration):
	#daily wise
	if req_duration=='daily':
		req_duration={
				"date_histogram" : {
					"field" : "plannedProductionDate",
						"fixed_interval": "1d"
						}
						}
	#hour wise
	elif req_duration=='hourly':
		req_duration={
				"date_histogram" : {
					"field" : "plannedProductionDate",
						"fixed_interval": "15m"
						}
						}
	#month wise trend:
	elif req_duration=='monthly':
		req_duration={
					"terms" : {
					"script" : {
					"source" : "InternalSqlScriptUtils.format(InternalQlScriptUtils.docValue(doc,params.v0),params.v1,params.v2)",
					"lang" : "painless",
					"params" : {
					"v0" : "plannedProductionDate",
					"v1" : "yyyy-MM",
					"v2" : "Z"
					}
					},
					"missing_bucket" : True,
					"value_type" : "string",
					"order" : "asc"
					}
					}
	# yearly wise trend:
	elif req_duration=='yearly':
		req_duration={
					"terms" : {
					"script" : {
					"source" : "InternalSqlScriptUtils.format(InternalQlScriptUtils.docValue(doc,params.v0),params.v1,params.v2)",
					"lang" : "painless",
					"params" : {
					"v0" : "plannedProductionDate",
					"v1" : "yyyy",
					"v2" : "Z"
					}
					},
					"missing_bucket" : True,
					"value_type" : "string",
					"order" : "asc"
					}
					}
	return req_duration

def column_ordering(self,coltype,colorder):
    if coltype !='undefined' and colorder !='undefined':
        if coltype == 'modifiedAt':
            sort=[
                   {
                     "modifiedAt" : {
                       "order" : colorder,
                       "missing" : "_first",
                       "unmapped_type" : "date"
                     }
                   }
                  ]
        else:
            sort=[
                   {
                     coltype+".keyword" : {
                       "order" : colorder,
                       "missing" : "_first",
                       "unmapped_type" : "keyword"
                     }
                   }
                  ]
    else :
        sort=[
                   {
                     "modifiedAt" : {
                       "order" : 'asc',
                       "missing" : "_first",
                       "unmapped_type" : "date"
                     }
                   }
                  ]
    return sort

class elk_data_extract():
#TOP Filters 1: plant
    def distinct_plant(self):
        #es = Elasticsearch(["http://elastic:elastic@172.31.158.159:9200/"])
        plant={}
        query={
				"size" : 0,
				"_source" : False,
				"aggregations" : {
					"groupby" : {
					"composite" : {
						"size" : 1000,
						"sources" : [
						{
							"plant_id" : {
							"terms" : {
								"field" : "shop.plant.id",
								"missing_bucket" : True,
								"order" : "asc"
							}
							}
						},
						{
							"plant_name" : {
							"terms" : {
								"field" : "shop.plant.name.keyword",
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
        res_vin=es.search(index=index_dropdown,body=query,size=10000,request_timeout=120)
        list=[]
        x2=res_vin['aggregations'].get('groupby').get('buckets')
        for i in x2:
            data={}
            data['plant_id']=i['key']['plant_id']
            data['plant_name']=i['key']['plant_name']
            list.append(data)
        print(list)
        return(list)
#TOP Filters 2: Shop Type
    def distinct_shopType(self,plant_id):
        #es = Elasticsearch(["http://elastic:elastic@172.31.158.159:9200/"])
        if plant_id != 'undefined':
            query={
				"size" : 0,
				"query" : {
					"term" : {
					"shop.plant.id" : {
						"value" : plant_id,
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
							"shop_type" : {
							"terms" : {
								"field" : "shop.type.keyword",
								"missing_bucket" : True,
								"order" : "asc"
							}
							}
						},
						{
							"plant_id" : {
							"terms" : {
								"field" : "shop.plant.id",
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
        else :
            query={
					"size" : 0,
					"_source" : False,
					"aggregations" : {
						"groupby" : {
						"composite" : {
							"size" : 1000,
							"sources" : [
							{
								"shop_type" : {
								"terms" : {
									"field" : "shop.type.keyword",
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
        res_vin=es.search(index=index_dropdown,body=query,size=10000,request_timeout=120)
        list=[]
        x2=res_vin['aggregations'].get('groupby').get('buckets')
        for i in x2:
            data={}
            #data['shop_type']=i['key']['shop_type']
            data['label']=i['key']['shop_type']
            #data['value']=i['key']['shop_type'].upper()
            data['value']=i['key']['shop_type']
            #data['plant_id']=i['key']['plant_id']
            list.append(data)
        return(list)
#TOP Filters 3: Shop
    def distinct_shop(self,plant_id,shop_type):
        #es = Elasticsearch(["http://elastic:elastic@172.31.158.159:9200/"])	
        #if plant_id != 'undefined':
        if plant_id != 'undefined' and 'undefined' in shop_type:
            query={
				"size" : 0,
				"query" : {
					"term" : {
					"shop.plant.id" : {
						"value" : plant_id,
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
							"shop_name" : {
							"terms" : {
								"field" : "shop.name.keyword",
								"missing_bucket" : True,
								"order" : "asc"
							}
							}
						},
						{
							"shop_id" : {
							"terms" : {
								"field" : "shop.id",
								"missing_bucket" : True,
								"order" : "asc"
							}
							}
						},
						{
							"plant_id" : {
							"terms" : {
								"field" : "shop.plant.id",
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
        #elif plant_id != 'undefined' and shop_type !='undefined':
        elif plant_id != 'undefined' and 'undefined' not in shop_type:
            query={
					"size" : 0,
					"query" : {
						"bool" : {
						"must" : [
							{
							"term" : {
								"shop.plant.id" : {
								"value" : plant_id,
								"boost" : 1.0
								}
							}
							},
                            {
							"terms" : {
								"shop.type.keyword" : shop_type
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
								"shop_name" : {
								"terms" : {
									"field" : "shop.name.keyword",
									"missing_bucket" : True,
									"order" : "asc"
								}
								}
							},
							{
								"shop_id" : {
								"terms" : {
									"field" : "shop.id",
									"missing_bucket" : True,
									"order" : "asc"
								}
								}
							},
							{
								"plant_id" : {
								"terms" : {
									"field" : "shop.plant.id",
									"missing_bucket" : True,
									"order" : "asc"
								}
								}
							},
							{
								"shop_type" : {
								"terms" : {
									"field" : "shop.type.keyword",
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
        else :
            query={
				"size" : 0,
				"_source" : False,
				"aggregations" : {
					"groupby" : {
					"composite" : {
						"size" : 1000,
						"sources" : [
						{
							"shop_name" : {
							"terms" : {
								"field" : "shop.name.keyword",
								"missing_bucket" : True,
								"order" : "asc"
							}
							}
						},
						{
							"shop_id" : {
							"terms" : {
								"field" : "shop.id",
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
        res_vin=es.search(index=index_dropdown,body=query,size=10000,request_timeout=120)
        list=[]
        x2=res_vin['aggregations'].get('groupby').get('buckets')
        for i in x2:
            data={}
            #data['shop_name']=i['key']['shop_name']
            #data['shop_id']=i['key']['shop_id']
            data['label']=i['key']['shop_name']
            data['value']=i['key']['shop_id']
            list.append(data)
        return(list)
#TOP Filters 4: Status
    def distinct_status(self,plant_id,shop_type):
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
								"plant_id.keyword" : {
								"value" : plant_id,
								"boost" : 1.0
								}
							}
							},
							{
							"terms" : {
								"shop_type.keyword" : shop_type
							}
							}
						],
                               "adjust_pure_negative" : True,
                               "boost" : 1.0
                             }
                           },
                           {
                             "term" : {
                               "status_condation.keyword" : {
                                 "value" : "active",
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
                               "status_name" : {
                                 "terms" : {
                                   "field" : "status_name.keyword",
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
        #es = Elasticsearch(["http://elastic:elastic@172.31.158.159:9200/"])	
        print(query)
        res_vin=es.search(index=index_status,body=query,size=10000,request_timeout=120)
        list=[]
        x2=res_vin['aggregations'].get('groupby').get('buckets')
        for i in x2:
            data={}
            data['status_name']=i['key']['status_name']
            list.append(data)
        custom_order = ['PLANNED', 'DROPPED', 'BOOKED', 'WTCL', 'ROLLED','SFLT']
        # Sort the list using the custom order
        sorted_list = sorted(list, key=lambda x: custom_order.index(x['status_name']))
        return(sorted_list)
#TOP Filters 5: shiftname
    def distinct_shiftname(self):
        #es = Elasticsearch(["http://elastic:elastic@172.31.158.159:9200/"])
        query={
			"size" : 0,
			"query" : {
				"bool" : {
				"must" : [
					{
					"exists" : {
						"field" : "shiftName",
						"boost" : 1.0
					}
					},
					{
					"terms" : {
						"shiftName.keyword" : [
						"A",
						"B",
						"C"
						],
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
						"shiftName" : {
						"terms" : {
							"field" : "shiftName.keyword",
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
        res_vin=es.search(index="flat_mfg_produced_material_v1_kafka",body=query,size=10000,request_timeout=120)
        list=[]
        x2=res_vin['aggregations'].get('groupby').get('buckets')
        for i in x2:
            data={}
            data['shiftName']=i['key']['shiftName']
            list.append(data)
        return(list)

#TOP Filters 6: Distinct Line
    def distinct_line(self,plant_id,shop_type,shop):
        #es = Elasticsearch(["http://elastic:elastic@172.31.158.159:9200/"])
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
                                 "field" : "id",
                                 "boost" : 1.0
                               }
                             },
                             {
                               "term" : {
                                 "shop.plant.id" : {
                                   "value" : plant_id,
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
                         "terms" : {
                           "shop.type.keyword" : shop_type
                         }
                       },
                                              {
                         "terms" : {
                           "shop.id" : shop
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
                           "line_id" : {
                             "terms" : {
                               "field" : "id",
                               "missing_bucket" : True,
                               "order" : "asc"
                             }
                           }
                         },
                         {
                           "line_name" : {
                             "terms" : {
                               "field" : "name.keyword",
                               "missing_bucket" : True,
                               "order" : "asc"
                             }
                           }
                         },
                         {
                           "description" : {
                             "terms" : {
                               "field" : "description.keyword",
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
        res_vin=es.search(index=index_dropdown,body=query,size=10000,request_timeout=120)
        list=[]
        print(query)
        x2=res_vin['aggregations'].get('groupby').get('buckets')
        for i in x2:
            data={}
            data['line_id']=i['key']['line_id']
            data['line_name']=i['key']['line_name']
            list.append(data)
        print(list)
        return(list)

#table_view excel download
    def table_view_export(self,plant_id,shop_type,shop,Status,shift,line,from_date,to_date):
        print(plant_id,shop_type,shop,Status,shift,line,from_date,to_date)
        status=Status
        #es = Elasticsearch(hosts="http://elastic:elastic@172.31.158.159:9200/")
        primary_condations=[]
        if 'undefined' not in shop_type:
            primary_condations.append({
              "terms" : {
                "type.keyword" : shop_type
              }
            })              
        if 'undefined' not in shop:
            primary_condations.append({
              "terms" : {
                "shop.keyword" : shop
              }
            })  
        if 'undefined' not in line:
            primary_condations.append({
              "terms" : {
                "line.keyword" : line
              }
            })  
        if 'undefined' not in shift:
            primary_condations.append({
                                  "terms" : {
                                  "shiftName.keyword" : shift
                                  }
                              })   
        count_query={
                  "size" : 0,
                  "query" : {
                  "bool" : {
                  "must" : [
                  {
                  "bool" : {
                      "must" : [
                      {
                          "bool" : {
                          "must" : [
                              {
                              "term" : {
                                  "plant.keyword" : {
                                  "value" : plant_id,
                                  "boost" : 1.0
                                  }
                              }
                              },
                              {
                              "terms" : {
                                  "status.keyword" : status
                              }
                              }
                          ],
                          "adjust_pure_negative" : True,
                          "boost" : 1.0
                          }
                      },
                      {
                          "bool" : {
                          "must" : primary_condations,
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
                  "bool" : {
                      "must" : [
                      {
                          "range" : {
                          "modifiedAt" : {
                          "from" : from_date,
                          "to" : to_date,
                          "include_lower" : True,
                          "include_upper" : True,
                          "time_zone": "Z",
                          "boost" : 1.0,
                          "format":"yyyy-MM-dd HH:mm:ss"
                          }
                          }
                      },
                                                        {
                  "bool" : {
                  "must_not" : [
                  {
                  "term" : {
                    "status.keyword" : {
                      "value" : "ASN_GENERATED",
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
        query={
                       "size" : 1000,
                       "query" : {
                       "bool" : {
                       "must" : [
                       {
                       "bool" : {
                           "must" : [
                           {
                               "bool" : {
                               "must" : [
                                   {
                                   "term" : {
                                       "plant.keyword" : {
                                       "value" : plant_id,
                                       "boost" : 1.0
                                       }
                                   }
                                   },
                                   {
                                   "terms" : {
                                  "status.keyword" : status
                              }
                                   }
                               ],
                               "adjust_pure_negative" : True,
                               "boost" : 1.0
                               }
                           },
                           {
                               "bool" : {
                               "must" : primary_condations,
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
                       "bool" : {
                           "must" : [
                           {
                               "range" : {
                               "modifiedAt" : {
                               "from" : from_date,
                               "to" : to_date,
                               "include_lower" : True,
                               "include_upper" : True,
                               "time_zone": "Z",
                               "boost" : 1.0,
                               "format":"yyyy-MM-dd HH:mm:ss"
                               }
                               }
                           },
                                                             {
                       "bool" : {
                       "must_not" : [
                       {
                       "term" : {
                         "status.keyword" : {
                           "value" : "ASN_GENERATED",
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
                       }
                       ],
                       "adjust_pure_negative" : True,
                       "boost" : 1.0
                       }
                       },
                       "_source" : {
                       "includes" : [
                           "plant",
                           "shop",
                           "materialNumber",
                           "asnId",
                           "shiftName",
                           "status",
                           "serialNumber",
                           "type",
                           "modifiedAt"
                       ],
                       "excludes" : [ ]
                       },
                       "docvalue_fields" : [
                                   {
                                   "field" : "modifiedAt",
                                   "format" : "yyyy-MM-dd HH:mm:ss"
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
        #name=index_name(self,from_date,to_date)
        search_df=es.search(index=index_name,body=count_query,request_timeout=120)
        count=search_df['hits']['total']['value']
        res_vin=es.search(index=index_name,body=query,size=count,request_timeout=120)
        x2=res_vin['hits'].get('hits')
        list=[]
        for i in x2:
            del i['_index']
            del i['_type']
            del i['_id']
            del i['_score']
            dict=i['_source']
            list.append(dict.copy())
        for i in list:
            if len(i['modifiedAt']) == 20:
                i['modifiedAt']=datetime.strptime(i['modifiedAt'],'%Y-%m-%dT%H:%M:%SZ')
            else:
                i['modifiedAt']=datetime.strptime(i['modifiedAt'],'%Y-%m-%dT%H:%M:%S.%fZ')
            i['modifiedAt']=i['modifiedAt'].strftime("%Y-%m-%d %H:%M:%S")
        vc_list=[]
        for i in list:
            vc_list.append(i['materialNumber'])
        vc_query={
                    "size" : 0,
                     "query" : {
                       "bool" : {
                         "must" : [
                           {
                             "term" : {
                               "plant.keyword" : {
                                 "value" : plant_id,
                                 "boost" : 1.0
                               }
                             }
                           },
                           {
                             "terms" : {
                               "vc_number.keyword" : vc_list
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
                              "vc_number" : {
                                "terms" : {
                                  "field" : "vc_number.keyword",
                                  "missing_bucket" : True,
                                  "order" : "asc"
                                }
                              }
                            },
                            {
                              "description" : {
                                "terms" : {
                                  "field" : "description.keyword",
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
        vc=es.search(index=index_vc,body=vc_query,size=count,request_timeout=120)
        x1=vc['aggregations'].get('groupby').get('buckets')
        list1=[]
        for i in x1:
          dict_vin=i['key']
          list1.append(dict_vin.copy())
        df=pd.DataFrame(list)
        df1=pd.DataFrame(list1)
        if not df1.empty:
            df2=sqldf("select vc_number as materialNumber,description from df1")
        else:
             df2=pd.DataFrame()
             df2['materialNumber']=[]
             df2['description']=[]
        df3 = df.merge(df2, on=['materialNumber'], how='left')
        print('df3',df3)
        df3 = df3.fillna('')
        dict = df3.to_dict('records')
        return dict
        #return list
#Graph 1:
    def plan_vs_actual_daily_count(self,plant_id,shop_type,shop,Status,shift,line,from_date,to_date):
        status=Status
        print(plant_id,shop_type,shop,status,shift,line,from_date,to_date)
        #es = Elasticsearch(["http://elastic:elastic@172.31.158.159:9200/"])
        #status_agg=status_conditions(self,status)
        print(status)
        status_agg={}
        for i in Status:
            status_agg.update({i : {"filter" : {"script" : {"script" : {"source" : "InternalQlScriptUtils.isNotNull(InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.eq(InternalQlScriptUtils.docValue(doc,params.v0),params.v1)) ? params.v2 : params.v3)","lang" : "painless","params" : {"v0" : "status.keyword","v1" : i,"v2" : 1,"v3" : 0}},"boost" : 1.0}}}})
        from_date_plan=datetime.strptime(from_date,'%Y-%m-%d %H:%M:%S')
        from_date_plan=datetime.strftime(from_date_plan,'%Y-%m-%d')
        to_date_plan=datetime.strptime(to_date,'%Y-%m-%d %H:%M:%S')
        to_date_plan=datetime.strftime(to_date_plan,'%Y-%m-%d')
        primary_condations=[]
        if 'undefined' not in shop_type:
            primary_condations.append({
              "terms" : {
                "type.keyword" : shop_type
              }
            })              
        if 'undefined' not in shop:
            primary_condations.append({
              "terms" : {
                "shop.keyword" : shop
              }
            })  
        if 'undefined' not in line:
            primary_condations.append({
              "terms" : {
                "line.keyword" : line
              }
            })
            
        additional_condations=[]
        if 'undefined' not in shift:
            additional_condations.append({
              "terms" : {
                "shiftName.keyword" : shift
              }
            })
            
        additional_condations.append(
						{
							"range" : {
						"modifiedAt" : {
						"from" : from_date,
						"to" : to_date,
						"include_lower" : True,
						"include_upper" : True,
						"time_zone": "Z",
						"boost" : 1.0,
                        "format":"yyyy-MM-dd HH:mm:ss"
						}
							}
						})
        plan_query={
			"size" : 0,
			"query" : {
				"bool" : {
				"must" : [
					{
					"bool" : {
						"must" : [
						{
							"bool" : {
							"must" : [
								{
								"term" : {
									"status.keyword" : {
									"value" : "ASN_GENERATED",
									"boost" : 1.0
									}
								}
								},
								{
								"term" : {
									"plant.keyword" : {
									"value" : plant_id,
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
							"must" : primary_condations,
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
					"bool" : {
						"must" : [
						#{
						#	"terms" : {
						#	"shiftName.keyword" : shift
						#	}
						#},
						{
							"range" : {
						"plannedProductionDate" : {
						"from" : from_date_plan,
						"to" : to_date_plan,
						"include_lower" : True,
						"include_upper" : True,
						"time_zone": "Z",
						"boost" : 1.0,
						"format":"yyyy-MM-dd"
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
					"plannedProductionDate" : {
						"date_histogram" : {
							"field" : "plannedProductionDate",
								"fixed_interval": "1d"
					}
					}
					},
					{
						"status_value" : {
						"terms" : {
							"field" : "status.keyword",
							"missing_bucket" : True,
							"order" : "asc"
						}
						}
					}
                    #      {
                    #    "line" : {
                    #      "terms" : {
                    #        "field" : "line.keyword",
                    #        "missing_bucket" : True,
                    #        "order" : "asc"
                    #      }
                    #    }
                    #   }
					]
				},
				"aggregations" : {
					"status" : {
					"filter" : {
						"exists" : {
						"field" : "status",
						"boost" : 1.0
						}
					}
					}
				}
				}
			}
			}
        actual_query={
			"size" : 0,
			"query" : {
				"bool" : {
				"must" : [
					{
					"bool" : {
						"must" : [
						{
							"bool" : {
							"must" : [
								{
								"terms" : {
									"status.keyword" : status
								}
								},
								{
								"term" : {
									"plant.keyword" : {
									"value" : plant_id,
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
							"must" : primary_condations,
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
					"bool" : {
						"must" : additional_condations,
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
									"modifiedAt" : {
										"date_histogram" : {
											"field" : "modifiedAt",
												"fixed_interval": "1d"
									}
									}
									},
													{
						"status_value" : {
						"terms" : {
							"field" : "status.keyword",
							"missing_bucket" : True,
							"order" : "asc"
						}
						}
					}
                    #     {
                    #    "line" : {
                    #      "terms" : {
                    #        "field" : "line.keyword",
                    #        "missing_bucket" : True,
                    #        "order" : "asc"
                    #      }
                    #    }
                    #    }
					]
				},
				"aggregations" : 
					status_agg
				}
			}
			}
        #name=index_name(self,from_date,to_date)
        res_vin1=es.search(index=index_name,body=plan_query,request_timeout=120)
        res_vin2=es.search(index=index_name,body=actual_query,request_timeout=120)
        print('plan_query',plan_query)
        x1=res_vin1['aggregations'].get('groupby').get('buckets')
        x2=res_vin2['aggregations'].get('groupby').get('buckets')
        #plan
        plan_modifiedlist=[]
        plan_countlist=[]
        plandict={}
        for i in x1:
            if i['key']['status_value']=="ASN_GENERATED":
                plan_modifiedlist.append(datetime.fromtimestamp(int(str(i['key']['plannedProductionDate'])[0:10])).strftime('%Y-%m-%d'))
                plan_countlist.append(i['doc_count'])
        plandict["x"]=plan_modifiedlist
        plandict["y"]=plan_countlist
        plandict["type"]="bar"
        plandict["name"]="PLANNED"
        plandict["marker"]= {'color': 'rgb(31, 119, 180)'}
        #drop
        drop_modifiedlist=[]
        drop_countlist=[]
        droppeddict={}
        for i in x2:
            if i['key']['status_value']=="DROPPED":
                drop_modifiedlist.append(datetime.fromtimestamp(int(str(i['key']['modifiedAt'])[0:10])).strftime('%Y-%m-%d'))
                drop_countlist.append(i['doc_count'])
        droppeddict["x"]=drop_modifiedlist
        droppeddict["y"]=drop_countlist
        droppeddict["type"]="bar"
        droppeddict["name"]="DROPPED"
        droppeddict["marker"]= {'color': 'rgb(255, 127, 14)'}
        #sflt
        sflt_modifiedlist=[]
        sflt_countlist=[]
        sfltdict={}
        for i in x2:
            if i['key']['status_value']=="SFLT":
                sflt_modifiedlist.append(datetime.fromtimestamp(int(str(i['key']['modifiedAt'])[0:10])).strftime('%Y-%m-%d'))
                sflt_countlist.append(i['doc_count'])
        sfltdict["x"]=sflt_modifiedlist
        sfltdict["y"]=sflt_countlist
        sfltdict["type"]="bar"
        sfltdict["name"]="SFLT"
        sfltdict["marker"]= {'color': 'rgb(148, 103, 189)'}
        #wtcl
        wtcl_modifiedlist=[]
        wtcl_countlist=[]
        wtcldict={}
        for i in x2:
            if i['key']['status_value']=="WTCL":
                wtcl_modifiedlist.append(datetime.fromtimestamp(int(str(i['key']['modifiedAt'])[0:10])).strftime('%Y-%m-%d'))
                wtcl_countlist.append(i['doc_count'])
        wtcldict["x"]=wtcl_modifiedlist
        wtcldict["y"]=wtcl_countlist
        wtcldict["type"]="bar"
        wtcldict["name"]="WTCL"
        wtcldict["marker"]= {'color': 'rgb(227, 119, 194)'}
        #rolled
        rolled_modifiedlist=[]
        rolled_countlist=[]
        rolleddict={}
        for i in x2:
            if i['key']['status_value']=="ROLLED":
                rolled_modifiedlist.append(datetime.fromtimestamp(int(str(i['key']['modifiedAt'])[0:10])).strftime('%Y-%m-%d'))
                rolled_countlist.append(i['doc_count'])
        rolleddict["x"]=rolled_modifiedlist
        rolleddict["y"]=rolled_countlist
        rolleddict["type"]="bar"
        rolleddict["name"]="ROLLED"
        rolleddict["marker"]= {'color': 'rgb(44, 160, 44)'}
        #booked
        booked_modifiedlist=[]
        booked_countlist=[]
        bookeddict={}
        for i in x2:
            if i['key']['status_value']=="BOOKED":
                booked_modifiedlist.append(datetime.fromtimestamp(int(str(i['key']['modifiedAt'])[0:10])).strftime('%Y-%m-%d'))
                booked_countlist.append(i['doc_count'])
        bookeddict["x"]=booked_modifiedlist
        bookeddict["y"]=booked_countlist
        bookeddict["type"]="bar"
        bookeddict["name"]="BOOKED"
        bookeddict["marker"]= {'color': 'rgb(235,52,162)'}
        final_list=[]
        '''if plandict['x']!=[]:
            final_list.append(plandict)
        if droppeddict['x']!=[]:
            final_list.append(droppeddict)
        if bookeddict['x']!=[]:
            final_list.append(bookeddict)
        if wtcldict['x']!=[]:
            final_list.append(wtcldict)
        if sfltdict['x']!=[]:
            final_list.append(sfltdict)
        if rolleddict['x']!=[]:
            final_list.append(rolleddict)'''

        final_list.append(plandict)
        final_list.append(droppeddict)
        final_list.append(bookeddict)
        #final_list.append(wtcldict)
        final_list.append(rolleddict)
        final_list.append(sfltdict)
        
        print(final_list)
        for i in final_list:
            if not i['x']:
                i['x'] = [None]
            if not i['y']:
                i['y'] = [None]
        '''if all(not i['x'] and not i['y'] for i in final_list):
            final_list = []'''
        x_values = [item['x'] for item in final_list]
        y_values = [item['y'] for item in final_list]

        if all(val == [None] for val in x_values) and all(val == [None] for val in y_values):
            result = []
        else:
            result = final_list 
        return result

#Graph 2:
    def plan_vs_actual_hr_month_yr_count(self,plant_id,shop_type,shop,Status,shift,line,duration,req_duration,from_date,to_date):
        status=Status
        print(plant_id,shop_type,shop,status,shift,line,duration,from_date,to_date)
        timeperiod=duration
        from_date_plan=datetime.strptime(from_date,'%Y-%m-%d %H:%M:%S')
        from_date_plan=datetime.strftime(from_date_plan,'%Y-%m-%d')
        to_date_plan=datetime.strptime(to_date,'%Y-%m-%d %H:%M:%S')
        to_date_plan=datetime.strftime(to_date_plan,'%Y-%m-%d')
        #es = Elasticsearch(["http://elastic:elastic@172.31.158.159:9200/"])
        #status_agg=status_conditions(self,status)
        print(status)
        status_agg={}
        for i in Status:
            status_agg.update({i : {"filter" : {"script" : {"script" : {"source" : "InternalQlScriptUtils.isNotNull(InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.eq(InternalQlScriptUtils.docValue(doc,params.v0),params.v1)) ? params.v2 : params.v3)","lang" : "painless","params" : {"v0" : "status.keyword","v1" : i,"v2" : 1,"v3" : 0}},"boost" : 1.0}}}})
        duration=hr_month_yr(self,duration)
        req_duration=requried_hr_month_yr(self,req_duration)
        primary_condations=[]
        if 'undefined' not in shop_type:
            primary_condations.append({
              "terms" : {
                "type.keyword" : shop_type
              }
            })              
        if 'undefined' not in shop:
            primary_condations.append({
              "terms" : {
                "shop.keyword" : shop
              }
            })  
        if 'undefined' not in line:
            primary_condations.append({
              "terms" : {
                "line.keyword" : line
              }
            })
            
        additional_condations=[]
        if 'undefined' not in shift:
            additional_condations.append({
              "terms" : {
                "shiftName.keyword" : shift
              }
            })
            
        additional_condations.append(
        						{
        							"range" : {
        						"modifiedAt" : {
        						"from" : from_date,
        						"to" : to_date,
        						"include_lower" : True,
        						"include_upper" : True,
        						"time_zone": "Z",
        						"boost" : 1.0,
                                "format":"yyyy-MM-dd HH:mm:ss"
        						}
        							}
        						})
        plan_query={
			"size" : 0,
			"query" : {
				"bool" : {
				"must" : [
					{
					"bool" : {
						"must" : [
						{
							"bool" : {
							"must" : [
								{
								"term" : {
									"status.keyword" : {
									"value" : "ASN_GENERATED",
									"boost" : 1.0
									}
								}
								},
								{
								"term" : {
									"plant.keyword" : {
									"value" : plant_id,
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
							"must" : primary_condations,
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
					"bool" : {
						"must" : [
						#{
						#	"terms" : {
						#	"shiftName.keyword" : shift
						#	}
						#},
						{
							"range" : {
						"plannedProductionDate" : {
						"from" : from_date_plan,
						"to" : to_date_plan,
						"include_lower" : True,
						"include_upper" : True,
						"time_zone": "Z",
						"boost" : 1.0,
						"format":"yyyy-MM-dd"
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
					"plannedProductionDate" : req_duration
					},
					{
						"status_value" : {
						"terms" : {
							"field" : "status.keyword",
							"missing_bucket" : True,
							"order" : "asc"
						}
						}
					}
                        # {
                        #  "line" : {
                        #    "terms" : {
                        #      "field" : "line.keyword",
                        #      "missing_bucket" : True,
                        #      "order" : "asc"
                        #    }
                        #  }
                        #  }
					]
				},
				"aggregations" : {
					"status" : {
					"filter" : {
						"exists" : {
						"field" : "status",
						"boost" : 1.0
						}
					}
					}
				}
				}
			}
			}
        actual_query={
			"size" : 0,
			"query" : {
				"bool" : {
				"must" : [
					{
					"bool" : {
						"must" : [
						{
							"bool" : {
							"must" : [
								{
								"terms" : {
									"status.keyword" : status
								}
								},
								{
								"term" : {
									"plant.keyword" : {
									"value" : plant_id,
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
							"must" : primary_condations,
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
					"bool" : {
						"must" : additional_condations,
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
								"modifiedAt" : duration
								},
													{
						"status_value" : {
						"terms" : {
							"field" : "status.keyword",
							"missing_bucket" : True,
							"order" : "asc"
						}
						}
					}
                        # {
                        # "line" : {
                        #   "terms" : {
                        #     "field" : "line.keyword",
                        #     "missing_bucket" : True,
                        #     "order" : "asc"
                        #   }
                        # }
                        # }
					]
				},
				"aggregations" : 
					status_agg
				}
			}
			}
        #name=index_name(self,from_date,to_date)
        res_vin1=es.search(index=index_name,body=plan_query,request_timeout=120)
        res_vin2=es.search(index=index_name,body=actual_query,request_timeout=120)
        print('actual_query',actual_query)
        x1=res_vin1['aggregations'].get('groupby').get('buckets')
        x2=res_vin2['aggregations'].get('groupby').get('buckets')
        #plan
        req_xaxis=[]
        plan_modifiedlist=[]
        plan_countlist=[]
        plandict={}
        #drop
        drop_modifiedlist=[]
        drop_countlist=[]
        droppeddict={}
        for i in x2:
            if i['key']['status_value']=="DROPPED":
                if timeperiod == 'hourly':
                    modified_at = i['key']['modifiedAt'] / 1000.0  # converting milliseconds to seconds
                    dt = datetime.fromtimestamp(modified_at,tz=timezone)
                    drop_modifiedlist.append(dt.strftime('%Y-%m-%d %H:%M:%S'))
                    #drop_modifiedlist.append(datetime.fromtimestamp(int(str(i['key']['modifiedAt'])[0:10])).strftime('%Y-%m-%d %H:%M:%S'))
                elif timeperiod == 'daily':
                    drop_modifiedlist.append(datetime.fromtimestamp(int(str(i['key']['modifiedAt'])[0:10])).strftime('%Y-%m-%d'))
                else:
                    drop_modifiedlist.append(i['key']['modifiedAt'])
                drop_countlist.append(i['doc_count'])
        req_xaxis.extend(drop_modifiedlist)
        droppeddict["x"]=drop_modifiedlist
        droppeddict["y"]=drop_countlist
        droppeddict['type']='scatter'
        droppeddict['mode']='lines+markers+text'
        droppeddict["name"]="DROPPED"
        droppeddict["marker"]= {'color': 'rgb(255, 127, 14)'}
        #sflt
        sflt_modifiedlist=[]
        sflt_countlist=[]
        sfltdict={}
        for i in x2:
            if i['key']['status_value']=="SFLT":
                if timeperiod == 'hourly':
                    modified_at = i['key']['modifiedAt'] / 1000.0  # converting milliseconds to seconds
                    dt = datetime.fromtimestamp(modified_at,tz=timezone)
                    sflt_modifiedlist.append(dt.strftime('%Y-%m-%d %H:%M:%S'))
                    #sflt_modifiedlist.append(datetime.fromtimestamp(int(str(i['key']['modifiedAt'])[0:10])).strftime('%Y-%m-%d %H:%M:%S'))
                elif timeperiod == 'daily':
                    sflt_modifiedlist.append(datetime.fromtimestamp(int(str(i['key']['modifiedAt'])[0:10])).strftime('%Y-%m-%d'))
                else:
                    sflt_modifiedlist.append(i['key']['modifiedAt'])
                sflt_countlist.append(i['doc_count'])
        req_xaxis.extend(sflt_modifiedlist)
        sfltdict["x"]=sflt_modifiedlist
        sfltdict["y"]=sflt_countlist
        sfltdict['type']='scatter'
        sfltdict['mode']='lines+markers+text'
        sfltdict["name"]="SFLT"
        sfltdict["marker"]= {'color': 'rgb(148, 103, 189)'}
        #wtcl
        wtcl_modifiedlist=[]
        wtcl_countlist=[]
        wtcldict={}
        for i in x2:
            if i['key']['status_value']=="WTCL":
                if timeperiod == 'hourly':
                    modified_at = i['key']['modifiedAt'] / 1000.0  # converting milliseconds to seconds
                    dt = datetime.fromtimestamp(modified_at,tz=timezone)
                    wtcl_modifiedlist.append(dt.strftime('%Y-%m-%d %H:%M:%S'))
                    #wtcl_modifiedlist.append(datetime.fromtimestamp(int(str(i['key']['modifiedAt'])[0:10])).strftime('%Y-%m-%d %H:%M:%S'))
                elif timeperiod == 'daily':
                    wtcl_modifiedlist.append(datetime.fromtimestamp(int(str(i['key']['modifiedAt'])[0:10])).strftime('%Y-%m-%d'))
                else:
                    wtcl_modifiedlist.append(i['key']['modifiedAt'])
                wtcl_countlist.append(i['doc_count'])
        req_xaxis.extend(wtcl_modifiedlist)
        wtcldict["x"]=wtcl_modifiedlist
        wtcldict["y"]=wtcl_countlist
        wtcldict['type']='scatter'
        wtcldict['mode']='lines+markers+text'
        wtcldict["name"]="WTCL"
        wtcldict["marker"]= {'color': 'rgb(227, 119, 194)'}
        #rolled
        rolled_modifiedlist=[]
        rolled_countlist=[]
        rolleddict={}
        for i in x2:
            if i['key']['status_value']=="ROLLED":
                if timeperiod == 'hourly':
                    modified_at = i['key']['modifiedAt'] / 1000.0  # converting milliseconds to seconds
                    dt = datetime.fromtimestamp(modified_at,tz=timezone)
                    rolled_modifiedlist.append(dt.strftime('%Y-%m-%d %H:%M:%S'))
                    #rolled_modifiedlist.append(datetime.fromtimestamp(int(str(i['key']['modifiedAt'])[0:10])).strftime('%Y-%m-%d %H:%M:%S'))
                elif timeperiod == 'daily':
                    rolled_modifiedlist.append(datetime.fromtimestamp(int(str(i['key']['modifiedAt'])[0:10])).strftime('%Y-%m-%d'))
                else:
                    rolled_modifiedlist.append(i['key']['modifiedAt'])
                rolled_countlist.append(i['doc_count'])
        req_xaxis.extend(rolled_modifiedlist)
        rolleddict["x"]=rolled_modifiedlist
        rolleddict["y"]=rolled_countlist
        rolleddict['type']='scatter'
        rolleddict['mode']='lines+markers+text'
        rolleddict["name"]="ROLLED"
        rolleddict["marker"]= {'color': 'rgb(44, 160, 44)'}
        for i in x1:
            if i['key']['status_value']=="ASN_GENERATED":
                if timeperiod == 'hourly':
                    plan_modifiedlist.append(datetime.fromtimestamp(int(str(i['key']['plannedProductionDate'])[0:10])).strftime('%Y-%m-%d'))
                elif timeperiod == 'daily':
                    plan_modifiedlist.append(datetime.fromtimestamp(int(str(i['key']['plannedProductionDate'])[0:10])).strftime('%Y-%m-%d'))
                else:
                    plan_modifiedlist.append(i['key']['plannedProductionDate'])
                plan_countlist.append(i['doc_count'])
        req_xaxis.extend(plan_modifiedlist)
        max_1=datetime.strptime(to_date,'%Y-%m-%d %H:%M:%S')
        max_1=datetime.strftime(max_1,'%Y-%m-%d %H:%M:%S')
        if from_date_plan == to_date_plan:
            for i in x1:
                    plan_countlist.append(i['doc_count'])
                    plan_modifiedlist.append(max_1)
        plandict["x"]=plan_modifiedlist
        plandict["y"]=plan_countlist
        plandict['type']='scatter'
        plandict['mode']='lines+markers+text'
        plandict["name"]="PLANNED"
        plandict["marker"]= {'color': 'rgb(31, 119, 180)'}
        #booked
        book_modifiedlist=[]
        book_countlist=[]
        bookeddict={}
        for i in x2:
            if i['key']['status_value']=="BOOKED":
                if timeperiod == 'hourly':
                    modified_at = i['key']['modifiedAt'] / 1000.0  # converting milliseconds to seconds
                    dt = datetime.fromtimestamp(modified_at,tz=timezone)
                    book_modifiedlist.append(dt.strftime('%Y-%m-%d %H:%M:%S'))
                    #drop_modifiedlist.append(datetime.fromtimestamp(int(str(i['key']['modifiedAt'])[0:10])).strftime('%Y-%m-%d %H:%M:%S'))
                elif timeperiod == 'daily':
                    book_modifiedlist.append(datetime.fromtimestamp(int(str(i['key']['modifiedAt'])[0:10])).strftime('%Y-%m-%d'))
                else:
                    book_modifiedlist.append(i['key']['modifiedAt'])
                book_countlist.append(i['doc_count'])
        req_xaxis.extend(book_modifiedlist)
        bookeddict["x"]=book_modifiedlist
        bookeddict["y"]=book_countlist
        bookeddict['type']='scatter'
        bookeddict['mode']='lines+markers+text'
        bookeddict["name"]="BOOKED"
        bookeddict["marker"]= {'color': 'rgb(235,52,162)'}
        final_list=[]
        #if plandict
        '''if plandict['x']!=[]:
            final_list.append(plandict)
        if droppeddict['x']!=[]:
            final_list.append(droppeddict)
        if bookeddict['x']!=[]:
            final_list.append(bookeddict)
        if wtcldict['x']!=[]:
            final_list.append(wtcldict)
        if sfltdict['x']!=[]:
            final_list.append(sfltdict)
        if rolleddict['x']!=[]:
            final_list.append(rolleddict)'''

        #final_list.append(plandict)
        final_list.append(droppeddict)
        final_list.append(bookeddict)
        #final_list.append(wtcldict)
        final_list.append(rolleddict)
        final_list.append(sfltdict)
        
        #final_dict = ChainMap(plan_dict,dropped_dict,sflt_dict,wtcl_dict,rolled_dict)
        #print(final_dict.maps)
        #print(final_list)
        for i in final_list:
            if not i['x']:
                i['x'] = [None]
            if not i['y']:
                i['y'] = [None]
        '''if all(not i['x'] and not i['y'] for i in final_list):
            final_list = []'''
        x_values = [item['x'] for item in final_list]
        y_values = [item['y'] for item in final_list]

        if all(val == [None] for val in x_values) and all(val == [None] for val in y_values):
            result = []
        else:
            result = final_list 
        return result

#custome table_view
    def table_view(self,plant_id,shop_type,shop,Status,shift,line,from_date,to_date,searchtext,start,colorder,coltype,size,shop_type_small):
        print(plant_id,shop_type,shop,Status,shift,line,from_date,to_date,searchtext,start,colorder,coltype,size)
        sort=column_ordering(self,coltype,colorder)
        status=Status
        #es = Elasticsearch(hosts="http://elastic:elastic@172.31.158.159:9200/")
        primary_condations=[]
        if 'undefined' not in shop_type:
            primary_condations.append({
              "terms" : {
                "type.keyword" : shop_type
              }
            })              
        if 'undefined' not in shop:
            primary_condations.append({
              "terms" : {
                "shop.keyword" : shop
              }
            })  
        if 'undefined' not in line:
            primary_condations.append({
              "terms" : {
                "line.keyword" : line
              }
            })  
        if 'undefined' not in shift:
            primary_condations.append({
                                  "terms" : {
                                  "shiftName.keyword" : shift
                                  }
                              })   
        
        #if searchtext=='undefined' :
        count_query={
		"size" : 0,
		"query" : {
			"bool" : {
			"must" : [
				{
				"bool" : {
					"must" : [
					{
						"bool" : {
						"must" : [
							{
							"term" : {
								"plant.keyword" : {
								"value" : plant_id,
								"boost" : 1.0
								}
							}
							},
							{
							"terms" : {
								"status.keyword" : status
							}
							}
						],
						"adjust_pure_negative" : True,
						"boost" : 1.0
						}
					},
					{
						"bool" : {
						"must" : primary_condations,
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
				"bool" : {
					"must" : [
					{
						"range" : {
						"modifiedAt" : {
						"from" : from_date,
						"to" : to_date,
						"include_lower" : True,
						"include_upper" : True,
						"time_zone": "Z",
						"boost" : 1.0,
                        "format":"yyyy-MM-dd HH:mm:ss"
						}
						}
					},
                                                      {
          "bool" : {
            "must_not" : [
              {
                "term" : {
                  "status.keyword" : {
                    "value" : "ASN_GENERATED",
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
        query={
        #"from": start,
		"size" : 1000,
		"query" : {
			"bool" : {
			"must" : [
				{
				"bool" : {
					"must" : [
					{
						"bool" : {
						"must" : [
							{
							"term" : {
								"plant.keyword" : {
								"value" : plant_id,
								"boost" : 1.0
								}
							}
							},
							{
							"terms" : {
								"status.keyword" : status
							}
							}
						],
						"adjust_pure_negative" : True,
						"boost" : 1.0
						}
					},
					{
						"bool" : {
						"must" : primary_condations,
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
				"bool" : {
					"must" : [
					{
						"range" : {
						"modifiedAt" : {
						"from" : from_date,
						"to" : to_date,
						"include_lower" : True,
						"include_upper" : True,
						"time_zone": "Z",
						"boost" : 1.0,
                        "format":"yyyy-MM-dd HH:mm:ss"
						}
						}
					},
                                                      {
          "bool" : {
            "must_not" : [
              {
                "term" : {
                  "status.keyword" : {
                    "value" : "ASN_GENERATED",
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
				}
			],
			"adjust_pure_negative" : True,
			"boost" : 1.0
			}
		},
		"_source" : {
			"includes" : [
					"plant",
					"shop",
					"materialNumber",
					"asnId",
					"shiftName",
					"status",
					"serialNumber",
					"type",
					"modifiedAt"
			],
			"excludes" : [ ]
			},
			"docvalue_fields" : [
							{
							"field" : "modifiedAt",
							"format" : "yyyy-MM-dd HH:mm:ss"
							}
							],
		"sort" : sort
		}
        #name=index_name(self,from_date,to_date)
        print('index_name',index_name)
        print('es',es)
        search_df=es.search(index=index_name,body=count_query,request_timeout=120)
        count=search_df['hits']['total']['value']
        res_vin=es.search(index=index_name,body=query,size=count,request_timeout=120)
        x2=res_vin['hits'].get('hits')
        '''elif '-' in searchtext  or ':' in searchtext:
            print('searchtext is  a date')
            count_query={
  "size" : 0,
  "query" : {
    "bool" : {
      "must" : [
        {
          "bool" : {
            "must" : [
              {
                "bool" : {
                  "must" : [
                    {
                      "term" : {
                        "plant.keyword" : {
                          "value" : plant_id,
                          "boost" : 1.0
                        }
                      }
                    },
                {
                "terms" : {
                    "status.keyword" : status
                }
                }
                  ],
                  "adjust_pure_negative" : True,
                  "boost" : 1.0
                }
              },
              {
                "bool" : {
                  "must" : primary_condations,
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
          "bool" : {
            "must" : [
              {
                "bool" : {
                  "must" : [
                    {
                      "term" : {
                        "modifiedAt" : {
                          "value" : searchtext,
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
						"modifiedAt" : {
						"from" : from_date,
						"to" : to_date,
						"include_lower" : True,
						"include_upper" : True,
						"time_zone": "Z",
						"boost" : 1.0,
                        "format":"yyyy-MM-dd HH:mm:ss"
						}
                }
              },
                                      {
          "bool" : {
            "must_not" : [
              {
                "term" : {
                  "status.keyword" : {
                    "value" : "ASN_GENERATED",
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
            query={
  "from" : start,
  "size": size,
  "query" : {
    "bool" : {
      "must" : [
        {
          "bool" : {
            "must" : [
              {
                "bool" : {
                  "must" : [
                    {
                      "term" : {
                        "plant.keyword" : {
                          "value" : plant_id,
                          "boost" : 1.0
                        }
                      }
                    },
                {
                "terms" : {
                    "status.keyword" : status
                }
                }
                  ],
                  "adjust_pure_negative" : True,
                  "boost" : 1.0
                }
              },
              {
                "bool" : {
                  "must" : primary_condations,
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
          "bool" : {
            "must" : [
              {
                "bool" : {
                  "must" : [
                    {
                      "term" : {
                        "modifiedAt" : {
                          "value" : searchtext,
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
						"modifiedAt" : {
						"from" : from_date,
						"to" : to_date,
						"include_lower" : True,
						"include_upper" : True,
						"time_zone": "Z",
						"boost" : 1.0,
                        "format":"yyyy-MM-dd HH:mm:ss"
						}
                }
              },
                                      {
          "bool" : {
            "must_not" : [
              {
                "term" : {
                  "status.keyword" : {
                    "value" : "ASN_GENERATED",
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
        }
      ],
      "adjust_pure_negative" : True,
      "boost" : 1.0
    }
  },
  "_source" : {
			"includes" : [
					"plant",
					"shop",
					"materialNumber",
					"asnId",
					"shiftName",
					"status",
					"serialNumber",
					"type",
					"modifiedAt"
			],
			"excludes" : [ ]
			},
			"docvalue_fields" : [
							{
							"field" : "modifiedAt",
							"format" : "yyyy-MM-dd HH:mm:ss"
							}
							],
  "sort" : sort
}
            name=index_name(self,from_date,to_date)
            search_df=es.search(index=name,body=count_query,request_timeout=120)
            count=search_df['hits']['total']['value']
            res_vin=es.search(index=name,body=query,request_timeout=120)
            x2=res_vin['hits'].get('hits')
        else :
            print('searchtext is not a date')
            count_query={
  "size" : 0,
  "query" : {
    "bool" : {
      "must" : [
        {
          "bool" : {
            "must" : [
              {
                "bool" : {
                  "must" : [
                    {
                      "term" : {
                        "plant.keyword" : {
                          "value" : plant_id,
                          "boost" : 1.0
                        }
                      }
                    },
                {
                "terms" : {
                    "status.keyword" : status
                }
                }
                  ],
                  "adjust_pure_negative" : True,
                  "boost" : 1.0
                }
              },
              {
                "bool" : {
                  "must" : primary_condations,
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
          "bool" : {
            "must" : [
              {
                "bool" : {
                  "must" : [
                    {
                      "bool" : {
                        "should" : [
                          {
                            "bool" : {
                              "should" : [
                                {
                                  "bool" : {
                                    "should" : [
                                      {
                                        "bool" : {
                                          "should" : [
                                            {
                                              "bool" : {
                                                "should" : [
                                                  {
                                                    "bool" : {
                                                      "should" : [
                                                        {
                                                          "bool" : {
                                                            "should" : [
                                                              {
                                                                "term" : {
                                                                  "plant.keyword" : {
                                                                    "value" : searchtext,
                                                                    "boost" : 1.0
                                                                  }
                                                                }
                                                              },
                                                              {
                                                                "term" : {
                                                                  "shop.keyword" : {
                                                                    "value" : searchtext,
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
                                                          "term" : {
                                                            "materialNumber.keyword" : {
                                                              "value" : searchtext,
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
                                                    "term" : {
                                                      "asnId.keyword" : {
                                                        "value" : searchtext,
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
                                              "term" : {
                                                "shiftName.keyword" : {
                                                  "value" : searchtext,
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
                                        "term" : {
                                          "status.keyword" : {
                                            "value" : searchtext,
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
                                  "term" : {
                                    "serialNumber.keyword" : {
                                      "value" : searchtext,
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
                            "term" : {
                              "type.keyword" : {
                                "value" : searchtext,
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
						"modifiedAt" : {
						"from" : from_date,
						"to" : to_date,
						"include_lower" : True,
						"include_upper" : True,
						"time_zone": "Z",
						"boost" : 1.0,
                        "format":"yyyy-MM-dd HH:mm:ss"
						}
                }
              },
                                      {
          "bool" : {
            "must_not" : [
              {
                "term" : {
                  "status.keyword" : {
                    "value" : "ASN_GENERATED",
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
            query={
  "from" : start,
  "size": size,
  "query" : {
    "bool" : {
      "must" : [
        {
          "bool" : {
            "must" : [
              {
                "bool" : {
                  "must" : [
                    {
                      "term" : {
                        "plant.keyword" : {
                          "value" : plant_id,
                          "boost" : 1.0
                        }
                      }
                    },
                {
                "terms" : {
                    "status.keyword" : status
                }
                }
                  ],
                  "adjust_pure_negative" : True,
                  "boost" : 1.0
                }
              },
              {
                "bool" : {
                  "must" : primary_condations,
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
          "bool" : {
            "must" : [
              {
                "bool" : {
                  "must" : [
                    {
                      "bool" : {
                        "should" : [
                          {
                            "bool" : {
                              "should" : [
                                {
                                  "bool" : {
                                    "should" : [
                                      {
                                        "bool" : {
                                          "should" : [
                                            {
                                              "bool" : {
                                                "should" : [
                                                  {
                                                    "bool" : {
                                                      "should" : [
                                                        {
                                                          "bool" : {
                                                            "should" : [
                                                              {
                                                                "term" : {
                                                                  "plant.keyword" : {
                                                                    "value" : searchtext,
                                                                    "boost" : 1.0
                                                                  }
                                                                }
                                                              },
                                                              {
                                                                "term" : {
                                                                  "shop.keyword" : {
                                                                    "value" : searchtext,
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
                                                          "term" : {
                                                            "materialNumber.keyword" : {
                                                              "value" : searchtext,
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
                                                    "term" : {
                                                      "asnId.keyword" : {
                                                        "value" : searchtext,
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
                                              "term" : {
                                                "shiftName.keyword" : {
                                                  "value" : searchtext,
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
                                        "term" : {
                                          "status.keyword" : {
                                            "value" : searchtext,
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
                                  "term" : {
                                    "serialNumber.keyword" : {
                                      "value" : searchtext,
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
                            "term" : {
                              "type.keyword" : {
                                "value" : searchtext,
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
						"modifiedAt" : {
						"from" : from_date,
						"to" : to_date,
						"include_lower" : True,
						"include_upper" : True,
						"time_zone": "Z",
						"boost" : 1.0,
                        "format":"yyyy-MM-dd HH:mm:ss"
						}
                }
              },
                                      {
          "bool" : {
            "must_not" : [
              {
                "term" : {
                  "status.keyword" : {
                    "value" : "ASN_GENERATED",
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
        }
      ],
      "adjust_pure_negative" : True,
      "boost" : 1.0
    }
  },
  "_source" : {
			"includes" : [
					"plant",
					"shop",
					"materialNumber",
					"asnId",
					"shiftName",
					"status",
					"serialNumber",
					"type",
					"modifiedAt"
			],
			"excludes" : [ ]
			},
			"docvalue_fields" : [
							{
							"field" : "modifiedAt",
							"format" : "yyyy-MM-dd HH:mm:ss"
							}
							],
  "sort" : sort
}
            name=index_name(self,from_date,to_date)
            search_df=es.search(index=name,body=count_query,request_timeout=120)
            count=search_df['hits']['total']['value']
            res_vin=es.search(index=name,body=query,request_timeout=120)
            x2=res_vin['hits'].get('hits')'''
        list=[]
        for i in x2:
            del i['_index']
            del i['_type']
            del i['_id']
            del i['_score']
            dict=i['_source']
            list.append(dict.copy())
        for i in list:
            if len(i['modifiedAt']) == 20:
                i['modifiedAt']=datetime.strptime(i['modifiedAt'],'%Y-%m-%dT%H:%M:%SZ')
            else:
                i['modifiedAt']=datetime.strptime(i['modifiedAt'],'%Y-%m-%dT%H:%M:%S.%fZ')
            i['modifiedAt']=i['modifiedAt'].strftime("%Y-%m-%d %H:%M:%S")
        vc_list=[]
        for i in list:
            vc_list.append(i['materialNumber'])
        vc_query={
                    "size" : 0,
                     "query" : {
                       "bool" : {
                         "must" : [
                           {
                             "term" : {
                               "plant.keyword" : {
                                 "value" : plant_id,
                                 "boost" : 1.0
                               }
                             }
                           },
                           {
                             "terms" : {
                               "vc_number.keyword" : vc_list
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
                              "vc_number" : {
                                "terms" : {
                                  "field" : "vc_number.keyword",
                                  "missing_bucket" : True,
                                  "order" : "asc"
                                }
                              }
                            },
                            {
                              "description" : {
                                "terms" : {
                                  "field" : "description.keyword",
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
        vc=es.search(index=index_vc,body=vc_query,size=count,request_timeout=120)
        x1=vc['aggregations'].get('groupby').get('buckets')
        list1=[]
        for i in x1:
          dict_vin=i['key']
          list1.append(dict_vin.copy())
        df=pd.DataFrame(list)
        df1=pd.DataFrame(list1)
        if not df1.empty:
            df2=sqldf("select vc_number as materialNumber,description from df1")
        else:
             df2=pd.DataFrame()
             df2['materialNumber']=[]
             df2['description']=[]
        df3 = df.merge(df2, on=['materialNumber'], how='left')
        df3 = df3.fillna('')
        if searchtext !='undefined':
            query="""select * from df3 where shiftName like '%{0}%' or shop like '%{0}%' or serialNumber like '%{0}%' or materialNumber like '%{0}%' or modifiedAt like '%{0}%' or plant like '%{0}%' or asnId like '%{0}%' or type like '%{0}%' or status like '%{0}%' or description like '%{0}%' limit {1} ,{2}""".format(searchtext,start,size)
            df4=sqldf(query)
            count_query="""select count(*) as count from df3 where shiftName like '%{0}%' or shop like '%{0}%' or serialNumber like '%{0}%' or materialNumber like '%{0}%' or modifiedAt like '%{0}%' or plant like '%{0}%' or asnId like '%{0}%' or type like '%{0}%' or status like '%{0}%' or description like '%{0}%' """.format(searchtext)
            count=sqldf(count_query)
            print('count',count,count_query)
            count = count.values.tolist()
            print('count',count)
            dict = df4.to_dict('records')
            list_dict={}
            list_dict['result']=dict
            list_dict['count']=count
        else:
            query="""select * from df3 limit {0} ,{1}""".format(start,size)
            df3=sqldf(query)
            dict = df3.to_dict('records')
            list_dict={}
            list_dict['result']=dict
            list_dict['count']=count
        return list_dict