# Create your tests here.
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from datetime import date
from elasticsearch import Elasticsearch, helpers

from datetime import date
import pandas as pd
from datetime import datetime, timedelta
from datetime import datetime
#import datetime

import sys
sys.path.append("pv_api")
import db_connection
import cv_elk_indices
today = date.today()
today=str(today)

db_obj=db_connection.db_con()
es=db_obj.elk()

index_obj=cv_elk_indices.elk_indices()
name=index_obj.Reconciliation()
name_cnt_sum=index_obj.Reconciliation_cnt_sum()
name_status_transitions=index_obj.Reconciliation_status_transitions()

#es = Elasticsearch(["http://elastic:elastic@172.31.154.203:9200/"])

#def index_name(self):
#	index_name="flat_mfg_cv_reconciliation_transitions"
#	return index_name

class elk_data_extract():
      def distinct_status_supplier(self):
        #name=index_name(self)
        #es = Elasticsearch(["http://elastic:elastic@172.31.154.203:9200/"])
        query1={
            "size" : 0,
            "query" : {
              "bool" : {
                "must_not" : [
                  {
                    "term" : {
                      "status.keyword" : {
                        "value" : " ",
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
                          "field" : "status.keyword",
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
        query2={
                  "size" : 0,
                  "query" : {
                    "bool" : {
                      "must_not" : [
                        {
                          "term" : {
                            "supplier_name.keyword" : {
                              "value" : " ",
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
                            "supplier_name" : {
                              "terms" : {
                                "field" : "supplier_name.keyword",
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
        res_vin1=es.search(index=name,body=query1,request_timeout=120)
        res_vin2=es.search(index=name,body=query2,request_timeout=120)
        list1=[]
        list2=[]
        main_dict={}
        x1=res_vin1['aggregations'].get('groupby').get('buckets')
        x2=res_vin2['aggregations'].get('groupby').get('buckets')
        for i in x1:
            data={}
            data['status']=i['key']['status_name']
            list1.append(data)
        for i in x2:
            data={}
            data['supplier']=i['key']['supplier_name']
            list2.append(data)
        main_dict={}
        main_dict['status']=list1
        main_dict['supplier']=list2
        print(main_dict)
        return(main_dict)
      def distinct_businessUnit(self):
         #query=select businessUnit from combine_test_reconcalication_2 where businessUnit is not null  group by businessUnit
         #name=index_name(self)
         query={
                "size" : 0,
                "query" : {
                  "exists" : {
                    "field" : "business_unit",
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
                          "businessUnit" : {
                            "terms" : {
                              "field" : "business_unit.keyword",
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
         res_vin1=es.search(index=name,body=query,request_timeout=120)
         list1=[]
         x1=res_vin1['aggregations'].get('groupby').get('buckets')
         for i in x1:
             data={}
             data['businessUnit']=i['key']['businessUnit']
             list1.append(data)
         return(list1)
      
      #Graph 1:
      def Reconciliation_status_wise_count(self,from_date,to_date,status_name,supplier_name,bu):
            #name="flat_mfg_cv_reconciliation_status_wise_count_transitions"
            #es = Elasticsearch(["http://elastic:elastic@172.31.154.203:9200/"])
          # query= (select status,sum(tml_snapshot_vendorOutStandingBalanceTotal),count(reconciliationId) from flat_mfg_cv_reconciliation_status_wise_count_transitions where tml_snapshot_asOnDate>='2022-01' and tml_snapshot_asOnDate<='2023-02' and supplier_name in('APOLLO TYRES LTD') and status in('CANCELLED') and businessUnit='CVBU' group by status)

          #query= (select status,sum(CASE WHEN status ='IN_REVIEW' THEN 1 ELSE 0 END)as IN_REVIEW_cnt ,sum(CASE WHEN status ='COMPLETED' THEN 1 ELSE 0 END)as COMPLETED_cnt ,sum(CASE WHEN status ='CANCELLED' THEN 1 ELSE 0 END)as CANCELLED_cnt ,sum(CASE WHEN status ='PENDING' THEN 1 ELSE 0 END)as PENDING_cnt ,count(reconciliationId),count(status) from flat_mfg_cv_reconciliation_status_wise_count_transitions where tml_snapshot_asOnDate>='2022-01' and tml_snapshot_asOnDate<='2023-02' and supplier_name in('APOLLO TYRES LTD') and status in('CANCELLED') and businessUnit='CVBU' group by status)

          #query_sum="SELECT SUM(CASE WHEN status ='INITIATED' THEN tml_snapshot_vendorOutStandingBalanceTotal ELSE 0 END) AS INITIATED_sum,SUM(CASE WHEN status ='IN_REVIEW' THEN supplier_snapshot_vendorOutStandingBalanceTotal ELSE 0 END) AS IN_REVIEW_sum,SUM(CASE WHEN status ='COMPLETED' THEN supplier_snapshot_vendorOutStandingBalanceTotal ELSE 0 END) AS COMPLETED_sum,SUM(CASE WHEN status ='CANCELLED' THEN tml_snapshot_vendorOutStandingBalanceTotal ELSE 0 END) AS CANCELLED_sum,SUM(CASE WHEN status ='OUTDATED' THEN tml_snapshot_vendorOutStandingBalanceTotal ELSE 0 END) AS OUTDATED_sum,SUM(CASE WHEN status ='PENDING' THEN tml_snapshot_vendorOutStandingBalanceTotal ELSE 0 END) AS PENDING_sum FROM flat_mfg_cv_reconciliation_status_wise_count_transitions where tml_snapshot_asOnDate>='2022-01' and tml_snapshot_asOnDate<='2023-02' and supplier_name in('APOLLO TYRES LTD') and status in('CANCELLED') and businessUnit in('CVBU') GROUP BY status"
            primary_condations=[]
            if 'undefined' not in supplier_name:
                primary_condations.append({
                  "terms" : {
                    "supplier_name.keyword" : supplier_name
                  }
                })
            if 'undefined' not in status_name:
                primary_condations.append({
                  "terms" : {
                    "status.keyword" : status_name
                  }
                })
            
            additional_condations=[]
            if 'undefined' not in bu:
                additional_condations.append({
            "terms" : {
              "businessUnit.keyword" : bu
            }
                })
 
            additional_condations.append({
              "range" : {
                "tml_snapshot_asOnDate" : {
                  "from" : from_date,
                  "to" : to_date,
                  "include_lower" : True,
                  "include_upper" : True,
                  "time_zone" : "Z",
                  "boost" : 1.0,
                  "format":"yyyy-MM"
                }
              }
            })
            
            status_sum_agg={}
            for i in status_name:
                #if status is in(IN_REVIEW and COMPLETED use accountPayableSnapshot column for sum)
                if i =="IN_REVIEW" or i =="COMPLETED":
                    print("if")
                    status_sum_agg.update({i+"_Payable" : {
          "sum" : {
            "script" : {
              "source" : "InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.eq(InternalQlScriptUtils.docValue(doc,params.v0),params.v1)) ? InternalQlScriptUtils.docValue(doc,params.v2) : params.v3",
              "lang" : "painless",
              "params" : {
                "v0" : "status.keyword",
                "v1" : i,
                "v2" : "tml_snapshot_vendorOutStandingBalanceTotal",
                "v3" : 0
              }
            }
          }
        }})
                #else for remain all status use accountReceivableSnapshot column for sum
                else:
                    print("else")
                    status_sum_agg.update({i+"_Receivable" : {
          "sum" : {
            "script" : {
              "source" : "InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.eq(InternalQlScriptUtils.docValue(doc,params.v0),params.v1)) ? InternalQlScriptUtils.docValue(doc,params.v2) : params.v3",
              "lang" : "painless",
              "params" : {
                "v0" : "status.keyword",
                "v1" : i,
                "v2" : "supplier_snapshot_vendorOutStandingBalanceTotal",
                "v3" : 0
              }
            }
          }
        }})
            
            status_sum_agg.update({"count_id" : {
                      "filter" : {
                        "exists" : {
                          "field" : "reconciliationId",
                          "boost" : 1.0
                        }
                      }
                    }})

            status_sum_agg.update({"count_supplier" : {
                      "filter" : {
                        "exists" : {
                          "field" : "supplier_name",
                          "boost" : 1.0
                        }
                      }
                    }})
            status_per_agg={}
            for i in status_name:
                status_per_agg.update({i : {
                            "sum" : {
                              "script" : {
                                "source" : "InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.eq(InternalQlScriptUtils.docValue(doc,params.v0),params.v1)) ? params.v2 : params.v3",
                                "lang" : "painless",
                                "params" : {
                                  "v0" : "status.keyword",
                                  "v1" : i,
                                  "v2" : 1,
                                  "v3" : 0
                                }
                              }
                            }
                          }})
            status_per_agg.update({"total_cnt" : {
                            "filter" : {
                              "exists" : {
                                "field" : "status",
                                "boost" : 1.0
                              }
                            }
                          }
                          })
          
            
            query_sum={
              "size" : 0,
              "query" : {
                "bool" : {
                  "must" : [
                    {
                      "bool" : {
                        "must" : primary_condations,
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
                        "status_name" : {
                          "terms" : {
                            "field" : "status.keyword",
                            "missing_bucket" : True,
                            "order" : "asc"
                          }
                        }
                      }
                    ]
                  },
                  "aggregations" : status_sum_agg
                }
              }
            }
            
            query_per={
                    "size" : 0,
                    "query" : {
                      "bool" : {
                        "must" : [
                          {
                            "bool" : {
                              "must" : primary_condations,
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
                        "filters" : {
                          "filters" : [
                            {
                              "match_all" : {
                                "boost" : 1.0
                              }
                            }
                          ],
                          "other_bucket" : False,
                          "other_bucket_key" : "_other_"
                        },
                        "aggregations" : status_per_agg
                      }
                    }
                    }
            res_vin1=es.search(index=name_status_transitions,body=query_per,request_timeout=120)
            res_vin=es.search(index=name_status_transitions,body=query_sum,request_timeout=120)
            list1=[]
            x1=res_vin['aggregations'].get('groupby').get('buckets')
            x2=res_vin1['aggregations'].get('groupby').get('buckets')

            #add status of Receivable and Payable value sum in list1
            for i in x1:
                data={}
                data['status']=i['key']['status_name']
                data['count']=i['count_id']['doc_count']
                data['supplier_cnt']=i['count_supplier']['doc_count']
                data['sum'] = i.get(data['status']+"_Receivable", {}).get('value', 0.0) + i.get(data['status']+"_Payable", {}).get('value', 0.0)
                list1.append(data)
            print('list1',list1)
            list1 = [{'status': x['status'], 'count': x['count'],'supplier_cnt': x['supplier_cnt'], 'sum': round(float(abs(x['sum'])),2)} if x['sum'] < 0 
         else {'status': x['status'], 'count': x['count'],'supplier_cnt': x['supplier_cnt'], 'sum': round(float(x['sum']),2)} for x in list1]
            #calculate  percentage
            #total_docs = x2[0]['doc_count']
            #status_percentages = {}
            #for status in status_name:
            #    status_docs = x2[0][status]['value']
            #    status_percentages[status+'_per'] = round((status_docs / total_docs) * 100,2)
            #list1[sum] value if in "-ve" then make it "+ve"
            #for i in list1:
            #    if i['sum']<0:
            #      strvar=str(i['sum'])
            #      i['sum']=strvar[1:]
            #add percentage in final_list
            #final_list = []
            #for d in list1:
            #    status = d['status']
            #    percentage_key = f"{status.upper()}_per"
            #    percentage = status_percentages.get(percentage_key, 0.0)
            #    new_dict = {"status": status, "count": d["count"], "sum": d["sum"], "percentage": percentage}
            #    final_list.append(new_dict)
            return list1

      #Graph 2: Reconciliation_status_wise_count monthly wise
      def Reconciliation_status_wise_monthly_count(self,from_date,to_date,status_name,supplier_name,bu):
            #name=["flat_mfg_cv_reconciliation_status_wise_count_transitions"]
            #es = Elasticsearch(["http://elastic:elastic@172.31.154.203:9200/"])
            primary_condations=[]
            if 'undefined' not in supplier_name:
                primary_condations.append({
                  "terms" : {
                    "supplier_name.keyword" : supplier_name
                  }
                })
            if 'undefined' not in status_name:
                primary_condations.append({
                  "terms" : {
                    "status.keyword" : status_name
                  }
                })
            
            additional_condations=[]
            if 'undefined' not in bu:
                additional_condations.append({
            "terms" : {
              "businessUnit.keyword" : bu
            }
                })
 
            additional_condations.append({
              "range" : {
                "tml_snapshot_asOnDate" : {
                  "from" : from_date,
                  "to" : to_date,
                  "include_lower" : True,
                  "include_upper" : True,
                  "time_zone" : "Z",
                  "boost" : 1.0,
                  "format":"yyyy-MM"
                }
              }
            })
            
            status_sum_agg={}
            for i in status_name:
                #if status is in(IN_REVIEW and COMPLETED use accountPayableSnapshot column for sum)
                if i =="IN_REVIEW" or i =="COMPLETED":
                    print("if")
                    status_sum_agg.update({i+"_Payable" : {
          "sum" : {
            "script" : {
              "source" : "InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.eq(InternalQlScriptUtils.docValue(doc,params.v0),params.v1)) ? InternalQlScriptUtils.docValue(doc,params.v2) : params.v3",
              "lang" : "painless",
              "params" : {
                "v0" : "status.keyword",
                "v1" : i,
                "v2" : "tml_snapshot_vendorOutStandingBalanceTotal",
                "v3" : 0
              }
            }
          }
        }})
                #else for remain all status use accountReceivableSnapshot column for sum
                else:
                    print("else")
                    status_sum_agg.update({i+"_Receivable" : {
          "sum" : {
            "script" : {
              "source" : "InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.eq(InternalQlScriptUtils.docValue(doc,params.v0),params.v1)) ? InternalQlScriptUtils.docValue(doc,params.v2) : params.v3",
              "lang" : "painless",
              "params" : {
                "v0" : "status.keyword",
                "v1" : i,
                "v2" : "supplier_snapshot_vendorOutStandingBalanceTotal",
                "v3" : 0
              }
            }
          }
        }})
            
            status_sum_agg.update({"count_id" : {
                      "filter" : {
                        "exists" : {
                          "field" : "reconciliationId",
                          "boost" : 1.0
                        }
                      }
                    }})
            
            status_sum_agg.update({"count_supplier" : {
                      "filter" : {
                        "exists" : {
                          "field" : "supplier_name",
                          "boost" : 1.0
                        }
                      }
                    }})

            query_sum={
              "size" : 0,
              "query" : {
                "bool" : {
                  "must" : [
                    {
                      "bool" : {
                        "must" : primary_condations,
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
                        "statusname" : {
                          "terms" : {
                            "field" : "status.keyword",
                            "missing_bucket" : True,
                            "order" : "asc"
                          }
                        }
                      },
                      {
                              "tml_snapshot_asondate" : {
					                      "terms" : {
					                      "script" : {
					                      "source" : "InternalSqlScriptUtils.format(InternalQlScriptUtils.docValue(doc,params.v0),params.v1,params.v2)",
					                      "lang" : "painless",
					                      "params" : {
					                      "v0" : "tml_snapshot_asOnDate",
					                      "v1" : "yyyy-MM",
					                      "v2" : "Z"
					                      }
					                      },
					                      "missing_bucket" : True,
					                      "value_type" : "string",
					                      "order" : "asc"
					                      }
					                      }
                            }
                    ]
                  },
                  "aggregations" : status_sum_agg
                }
              }
            }
            res_vin=es.search(index=name_status_transitions,body=query_sum,request_timeout=120)
            list1=[]
            x1=res_vin['aggregations'].get('groupby').get('buckets')
            print("x1",x1)
            for i in x1:
                data={}
                data['x']=i['key']['tml_snapshot_asondate']
                data['status_name']=i['key']['statusname']
                data['id_count']=i['count_id']['doc_count']
                data['supplier_count']=i['count_supplier']['doc_count']
                data['sum'] = round(i.get(data['status_name']+"_Receivable", {}).get('value', 0.0),2) + round(i.get(data['status_name']+"_Payable", {}).get('value', 0.0),2)
                list1.append(data)
            print('list',list1)
            #list1[sum] value if in "-ve" then make it "+ve"
            list1 = [{'date': x['x'],'status': x['status_name'], 'id_count': x['id_count'], 'supplier_count': x['supplier_count'],'sum': float(abs(x['sum']))} if x['sum'] < 0 
         else {'date': x['x'],'status': x['status_name'], 'id_count': x['id_count'], 'supplier_count': x['supplier_count'],'sum': float(x['sum'])} for x in list1]
            print('list1',list1)

            final_list_dump=[]
            #IN_REVIEW
            inreview_datelist=[]
            inreview_countlist=[]
            inreview_sum=[]
            inreview_supplier_cnt=[]
            inreviewdict={}
            for i in list1:
                if i['status']=="IN_REVIEW":
                        inreview_datelist.append(i['date'])
                        inreview_countlist.append(i['id_count'])
                        inreview_sum.append(i['sum'])
                        inreview_supplier_cnt.append(i['supplier_count'])
            inreviewdict["x"]=inreview_datelist
            inreviewdict["y"]=inreview_countlist
            inreviewdict['sum']=inreview_sum
            inreviewdict['supplier_cnt']=inreview_supplier_cnt
            inreviewdict["status"]="IN_REVIEW"
            print('inreviewdict',inreviewdict)
            #INITIATED
            INITIATED_datelist=[]
            INITIATED_countlist=[]
            INITIATED_sum=[]
            INITIATED_supplier_cnt=[]
            INITIATEDdict={}
            for i in list1:
                if i['status']=="INITIATED":
                        INITIATED_datelist.append(i['date'])
                        INITIATED_countlist.append(i['id_count'])
                        INITIATED_sum.append(i['sum'])
                        INITIATED_supplier_cnt.append(i['supplier_count'])
            INITIATEDdict["x"]=INITIATED_datelist
            INITIATEDdict["y"]=INITIATED_countlist
            INITIATEDdict['sum']=INITIATED_sum
            INITIATEDdict['supplier_cnt']=INITIATED_supplier_cnt
            INITIATEDdict["status"]="INITIATED"

            #CANCELLED
            CANCELLED_datelist=[]
            CANCELLED_countlist=[]
            CANCELLED_sum=[]
            CANCELLED_supplier_cnt=[]
            CANCELLEDdict={}
            for i in list1:
                if i['status']=="CANCELLED":
                        CANCELLED_datelist.append(i['date'])
                        CANCELLED_countlist.append(i['id_count'])
                        CANCELLED_sum.append(i['sum'])
                        CANCELLED_supplier_cnt.append(i['supplier_count'])
            CANCELLEDdict["x"]=CANCELLED_datelist
            CANCELLEDdict["y"]=CANCELLED_countlist
            CANCELLEDdict['sum']=CANCELLED_sum
            CANCELLEDdict['supplier_cnt']=CANCELLED_supplier_cnt
            CANCELLEDdict["status"]="CANCELLED"
            print('CANCELLED_supplier_cnt',CANCELLED_supplier_cnt)
            #PENDING
            PENDING_datelist=[]
            PENDING_countlist=[]
            PENDING_sum=[]
            PENDING_supplier_cnt=[]
            PENDINGdict={}
            for i in list1:
                if i['status']=="PENDING":
                        PENDING_datelist.append(i['date'])
                        PENDING_countlist.append(i['id_count'])
                        PENDING_sum.append(i['sum'])
                        PENDING_supplier_cnt.append(i['supplier_count'])
            PENDINGdict["x"]=PENDING_datelist
            PENDINGdict["y"]=PENDING_countlist
            PENDINGdict['sum']=PENDING_sum
            PENDINGdict['supplier_cnt']=PENDING_supplier_cnt
            PENDINGdict["status"]="PENDING"
            
            #OUTDATED
            OUTDATED_datelist=[]
            OUTDATED_countlist=[]
            OUTDATED_sum=[]
            OUTDATED_supplier_cnt=[]
            OUTDATEDdict={}
            for i in list1:
                if i['status']=="OUTDATED":
                        OUTDATED_datelist.append(i['date'])
                        OUTDATED_countlist.append(i['id_count'])
                        OUTDATED_sum.append(i['sum'])
                        OUTDATED_supplier_cnt.append(i['supplier_count'])
            OUTDATEDdict["x"]=OUTDATED_datelist
            OUTDATEDdict["y"]=OUTDATED_countlist
            OUTDATEDdict['sum']=OUTDATED_sum
            OUTDATEDdict['supplier_cnt']=OUTDATED_supplier_cnt
            OUTDATEDdict["status"]="OUTDATED"

            #COMPLETED
            COMPLETED_datelist=[]
            COMPLETED_countlist=[]
            COMPLETED_sum=[]
            COMPLETED_supplier_cnt=[]
            COMPLETEDdict={}
            for i in list1:
                if i['status']=="COMPLETED":
                        COMPLETED_datelist.append(i['date'])
                        COMPLETED_countlist.append(i['id_count'])
                        COMPLETED_sum.append(i['sum'])
                        COMPLETED_supplier_cnt.append(i['supplier_count'])
            COMPLETEDdict["x"]=COMPLETED_datelist
            COMPLETEDdict["y"]=COMPLETED_countlist
            COMPLETEDdict['sum']=COMPLETED_sum
            COMPLETEDdict['supplier_cnt']=COMPLETED_supplier_cnt
            COMPLETEDdict["status"]="COMPLETED"

            final_list_dump.append(inreviewdict)
            final_list_dump.append(INITIATEDdict)
            final_list_dump.append(CANCELLEDdict)
            final_list_dump.append(PENDINGdict)
            final_list_dump.append(OUTDATEDdict)
            final_list_dump.append(COMPLETEDdict)
            print(final_list_dump)
            return final_list_dump

      #Graph 3:Aging Graph
      #query=select status,avg(time_diff) as no_days from flat_mfg_cv_reconciliation_transitions where businessUnit in('CVBU') and supplier_name in('FLEETGUARD FILTERS PVT. LTD.') and status in('PENDING') and tml_snapshot_asOnDate>='2023-01' and tml_snapshot_asOnDate<='2023-04' group by supplier_name,status,businessUnit
      def Aging_graph(self,from_date,to_date,status_name,supplier_name,bu):
            #name=index_name(self)
            primary_condations=[]
            if 'undefined' not in supplier_name:
                primary_condations.append({
                  "terms" : {
                    "supplier_name.keyword" : supplier_name
                  }
                })
            if 'undefined' not in status_name:
                primary_condations.append({
                  "terms" : {
                    "status.keyword" : status_name
                  }
                })
            
            additional_condations=[]
            if 'undefined' not in bu:
                additional_condations.append({
            "terms" : {
              "business_unit.keyword" : bu
            }
                })
            
            additional_condations.append({
              "range" : {
                "tml_snapshot_asondate" : {
                  "from" : from_date,
                  "to" : to_date,
                  "include_lower" : True,
                  "include_upper" : True,
                  "time_zone" : "Z",
                  "boost" : 1.0,
                  "format":"yyyy-MM"
                }
              }
            })
            
            query={
              "size" : 0,
              "query" : {
                "bool" : {
                  "must" : [
                    {
                      "bool" : {
                        "must" : primary_condations,
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
                        "supplier_name" : {
                          "terms" : {
                            "field" : "supplier_name.keyword",
                            "missing_bucket" : True,
                            "order" : "asc"
                          }
                        }
                      },
                      {
                        "status_name" : {
                          "terms" : {
                            "field" : "status.keyword",
                            "missing_bucket" : True,
                            "order" : "asc"
                          }
                        }
                      },
                      {
                        "businessUnit" : {
                          "terms" : {
                            "field" : "business_unit.keyword",
                            "missing_bucket" : True,
                            "order" : "asc"
                          }
                        }
                      }
                    ]
                  },
                  "aggregations" : {
                    "time_diff_days" : {
                      "avg" : {
                        "field" : "time_diff_days"
                      }
                    }
                  }
                }
              }
                }
            print(query)
            res_vin=es.search(index=name,body=query,request_timeout=120)
            print(res_vin)
            list=[]
            x1=res_vin['aggregations'].get('groupby').get('buckets')
            print('x1',x1)
            '''for i in x1:
                data={}
                data['status']=i['key']['status_name']
                data['time_diff_days']=i['time_diff_days']['value']
                list.append(data)'''
            merged_data = {}
            for i in x1:
              status = i['key']['status_name']
              time_diff_days = i['time_diff_days']['value']
              if status in merged_data:
                merged_data[status] += time_diff_days
              else:
                merged_data[status] = time_diff_days
            
            # Print the merged data with the sum of values
            result_list = [{'status': key, 'time_diff_days': value} for key, value in merged_data.items()]
            return(result_list)
            #return(list)
          
      #Graph 4:Analysis of Reconcalition completed vendors
      # query_sum_cnt= select category,COUNT(category) AS categoryCount,SUM(CASE WHEN category ='NOT_FOUND_SUPPLIER' THEN supplier_openitem_vendoroutstandingbalance ELSE 0 END) AS NOT_FOUND_SUPPLIER_sum ,sum(CASE WHEN category ='NOT_FOUND_TML' THEN tml_openitem_vendoroutstandingbalance ELSE 0 END) AS NOT_FOUND_TML_sum from flat_mfg_cv_reconciliation_transitions  where tml_snapshot_asOnDate>='2022-01' and tml_snapshot_asOnDate<='2023-02' and supplier_name in('APOLLO TYRES LTD')  and business_unit in('CVBU') group by category,supplier_snapshot_id,id'''

      #query_per= (select category,sum(CASE WHEN category ='MATCH' THEN 1 ELSE 0 END)as MATCH_cnt ,sum(CASE WHEN category ='MISMATCH' THEN 1 ELSE 0 END)as MISMATCH_cnt ,sum(CASE WHEN category ='NOT_FOUND_TML' THEN 1 ELSE 0 END)as NOT_FOUND_TML_cnt ,sum(CASE WHEN category ='NOT_FOUND_SUPPLIER' THEN 1 ELSE 0 END)as NOT_FOUND_SUPPLIER_cnt ,count(category) from flat_mfg_cv_reconciliation_transitions where tml_snapshot_asOnDate>='2022-01' and tml_snapshot_asOnDate<='2023-02' and supplier_name in('APOLLO TYRES LTD') and category in('NOT_FOUND_SUPPLIER','MATCH') and business_unit in('CVBU') group by category)
      def Reconciliation_completed_vendors(self,bu,supplier_name,from_date,to_date):
            print(bu,supplier_name,from_date,to_date)
            #name="flat_mfg_cv_reconciliation_differences_v1_idx"
            #name=index_name(self)
            #es = Elasticsearch(["http://elastic:elastic@172.31.154.203:9200/"])
            primary_condations=[]
            if 'undefined' not in supplier_name:
                primary_condations.append({
                  "terms" : {
                    "supplier_name.keyword" : supplier_name
                  }
                })
            if 'undefined' not in bu:
                primary_condations.append({
                  "terms" : {
                    "business_unit.keyword" : bu
                  }
                })
            additional_condations=[]
            #if 'undefined' not in status_name:
            #    additional_condations.append({
            #"terms" : {
            #  "status.keyword" : status_name
            #}
            #    })

            additional_condations.append({
              "range" : {
                "tml_snapshot_asondate" : {
                  "from" : from_date,
                  "to" : to_date,
                  "include_lower" : True,
                  "include_upper" : True,
                  "time_zone" : "Z",
                  "boost" : 1.0,
                  "format":"yyyy-MM"
                }
              }
            })
            #additional_condations.append({
            #          "term" : {
            #            "flag" : {
            #              "value" : "1",
            #              "boost" : 1.0
            #            }
            #          }
            #        })
            additional_condations.append({
                 "exists" : {
                   "field" : "category",
                   "boost" : 1.0
                   }
                 })
            category_sum_agg={}

            category_list=['NOT_FOUND_SUPPLIER','NOT_FOUND_TML','MATCH','MISMATCH']
            for i in category_list:
                if i=="NOT_FOUND_SUPPLIER":
                    category_sum_agg.update({i+"_Payable" : {
                         "sum" : {
                           "script" : {
                             "source" : "InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.eq(InternalQlScriptUtils.docValue(doc,params.v0),params.v1)) ? InternalQlScriptUtils.docValue(doc,params.v2) : params.v3",
                             "lang" : "painless",
                             "params" : {
                               "v0" : "category.keyword",
                               "v1" : i,
                               "v2" : "NOT_FOUND_SUPPLIER_sum",
                               "v3" : 0
                             }
                           }
                         }
                         }})
                else:
                    category_sum_agg.update({i+"_Receivable" : {
                         "sum" : {
                           "script" : {
                             "source" : "InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.eq(InternalQlScriptUtils.docValue(doc,params.v0),params.v1)) ? InternalQlScriptUtils.docValue(doc,params.v2) : params.v3",
                             "lang" : "painless",
                             "params" : {
                               "v0" : "category.keyword",
                               "v1" : i,
                               "v2" : "NOT_FOUND_TML_sum",
                               "v3" : 0
                             }
                           }
                         }
                         }})
                    
            category_sum_agg.update({"category_cnt" : {
          "sum" : {
            "field" : "count"
          }
        }})
            #print("category_sum_agg",category_sum_agg)

            category_per_agg={}
            for i in category_list:
                category_per_agg.update({i : {
                            "sum" : {
                              "script" : {
                                "source" : "InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.eq(InternalQlScriptUtils.docValue(doc,params.v0),params.v1)) ? params.v2 : params.v3",
                                "lang" : "painless",
                                "params" : {
                                  "v0" : "category.keyword",
                                  "v1" : i,
                                  "v2" : 1,
                                  "v3" : 0
                                }
                              }
                            }
                          }})
            category_per_agg.update({"total_cnt" : {
                            "filter" : {
                              "exists" : {
                                "field" : "category",
                                "boost" : 1.0
                              }
                            }
                          }
                          })
            
            query_sum={
                  "size" : 0,
                  "query" : {
                    "bool" : {
                      "must" : [
                        {
                          "bool" : {
                            "must" : primary_condations,
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
                            "category" : {
                              "terms" : {
                                "field" : "category.keyword",
                                "missing_bucket" : True,
                                "order" : "asc"
                              }
                            }
                          },
                          #{
                          #    "supplier_snapshot_id" : {
                          #      "terms" : {
                          #        "field" : "supplier_snapshot_id",
                          #        "missing_bucket" : True,
                          #        "order" : "asc"
                          #      }
                          #    }
                          # },
                            {
                              "tml_snapshot_asondate" : {
					                      "terms" : {
					                      "script" : {
					                      "source" : "InternalSqlScriptUtils.format(InternalQlScriptUtils.docValue(doc,params.v0),params.v1,params.v2)",
					                      "lang" : "painless",
					                      "params" : {
					                      "v0" : "tml_snapshot_asondate",
					                      "v1" : "yyyy-MM",
					                      "v2" : "Z"
					                      }
					                      },
					                      "missing_bucket" : True,
					                      "value_type" : "string",
					                      "order" : "asc"
					                      }
					                      }
                            }
                           #{
                           #  "id" : {
                           #    "terms" : {
                           #      "field" : "id",
                           #      "missing_bucket" : True,
                           #      "order" : "asc"
                           #    }
                           #  }
                           #}
                        ]
                      },
                      "aggregations" : category_sum_agg
                    }
                  }
            }
            
            query_per={
                  "size" : 0,
                  "query" : {
                    "bool" : {
                      "must" : [
                        {
                          "bool" : {
                            "must" : primary_condations,
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
                            "category" : {
                              "terms" : {
                                "field" : "category.keyword",
                                "missing_bucket" : True,
                                "order" : "asc"
                              }
                            }
                          },
                          {
                           "supplier_snapshot_id" : {
                             "terms" : {
                               "field" : "supplier_snapshot_id",
                               "missing_bucket" : True,
                               "order" : "asc"
                             }
                           }
                       },
                            {
                              "tml_snapshot_asondate" : {
					                      "terms" : {
					                      "script" : {
					                      "source" : "InternalSqlScriptUtils.format(InternalQlScriptUtils.docValue(doc,params.v0),params.v1,params.v2)",
					                      "lang" : "painless",
					                      "params" : {
					                      "v0" : "tml_snapshot_asondate",
					                      "v1" : "yyyy-MM",
					                      "v2" : "Z"
					                      }
					                      },
					                      "missing_bucket" : True,
					                      "value_type" : "string",
					                      "order" : "asc"
					                      }
					                      }
                            }
                       #{
                       #  "id" : {
                       #    "terms" : {
                       #      "field" : "id",
                       #      "missing_bucket" : True,
                       #      "order" : "asc"
                       #    }
                       #  }
                       #}
                        ]
                      },
                      "aggregations" : category_per_agg
                    }
                  }
            }

            #print(query_sum)
            #res_vin=es.search(index="flat_mfg_cv_count_sum",body=query_sum,request_timeout=120)
            res_vin=es.search(index=name_cnt_sum,body=query_sum,request_timeout=120)
            res_vin1=es.search(index=name,body=query_per,request_timeout=120)
            list1=[]
            x1=res_vin['aggregations'].get('groupby').get('buckets')
            x2=res_vin1['aggregations'].get('groupby').get('buckets')
            print("x1",x1)
            for i in x1:
                data={}
                data['x']=i['key']['tml_snapshot_asondate']
                data['category']=i['key']['category']
                data['count']=i['category_cnt']['value']
                data['sum'] = round(i.get(data['category']+"_Receivable", {}).get('value', 0.0),2) + round(i.get(data['category']+"_Payable", {}).get('value', 0.0),2)
                list1.append(data)
            #calculate  percentage
            total_docs = x2[0]['doc_count']
            category_percentages = {}
            for category in category_list:
                category_docs = x2[0][category]['value']
                category_percentages[category+'_per'] = round((category_docs / total_docs) * 100,2)
            #list1[sum] value if in "-ve" then make it "+ve"
            list1 = [{'date': x['x'],'category': x['category'], 'count': x['count'], 'sum': float(abs(x['sum']))} if x['sum'] < 0 
         else {'date': x['x'],'category': x['category'], 'count': x['count'], 'sum': float(x['sum'])} for x in list1]
            #add percentage in final_list
            final_list = []
            for d in list1:
                date=d['date']
                category = d['category']
                percentage_key = f"{category.upper()}_per"
                percentage = category_percentages.get(percentage_key, 0.0)
                new_dict = {"x":date,"y": d["count"],"name": category,  "sum": d["sum"], "percentage": percentage}
                final_list.append(new_dict)
            #print(final_list)
            final_list_dump=[]
            #Match
            match_datelist=[]
            match_countlist=[]
            match_sum=[]
            match_per=[]
            matchdict={}
            for i in final_list:
                if i['name']=="MATCH":
                        match_datelist.append(i['x'])
                        match_countlist.append(i['y'])
                        match_sum.append(i['sum'])
                        match_per.append(i['percentage'])
            matchdict["x"]=match_datelist
            matchdict["y"]=match_countlist
            matchdict['sum']=match_sum
            matchdict['per']=match_per
            matchdict["name"]="MATCH"

            #Mis-Match
            mismatch_datelist=[]
            mismatch_countlist=[]
            mismatch_sum=[]
            mismatch_per=[]
            mismatchdict={}
            for i in final_list:
                if i['name']=="MISMATCH":
                        mismatch_datelist.append(i['x'])
                        mismatch_countlist.append(i['y'])
                        mismatch_sum.append(i['sum'])
                        mismatch_per.append(i['percentage'])
            mismatchdict["x"]=mismatch_datelist
            mismatchdict["y"]=mismatch_countlist
            mismatchdict['sum']=mismatch_sum
            mismatchdict['per']=mismatch_per
            mismatchdict["name"]="MISMATCH"

            #NOT_FOUND_SUPPLIER
            supplier_datelist=[]
            supplier_countlist=[]
            supplier_sum=[]
            supplier_per=[]
            supplierdict={}
            for i in final_list:
                if i['name']=="NOT_FOUND_SUPPLIER":
                        supplier_datelist.append(i['x'])
                        supplier_countlist.append(i['y'])
                        supplier_sum.append(i['sum'])
                        supplier_per.append(i['percentage'])
            supplierdict["x"]=supplier_datelist
            supplierdict["y"]=supplier_countlist
            supplierdict['sum']=supplier_sum
            supplierdict['per']=supplier_per
            supplierdict["name"]="NOT_FOUND_SUPPLIER"

            #NOT_FOUND_TML
            tml_datelist=[]
            tml_countlist=[]
            tml_sum=[]
            tml_per=[]
            tmldict={}
            for i in final_list:
                if i['name']=="NOT_FOUND_TML":
                        tml_datelist.append(i['x'])
                        tml_countlist.append(i['y'])
                        tml_sum.append(i['sum'])
                        tml_per.append(i['percentage'])
            tmldict["x"]=tml_datelist
            tmldict["y"]=tml_countlist
            tmldict['sum']=tml_sum
            tmldict['per']=tml_per
            tmldict["name"]="NOT_FOUND_TML"
            

            final_list_dump.append(matchdict)
            final_list_dump.append(mismatchdict)
            final_list_dump.append(supplierdict)
            final_list_dump.append(tmldict)
            return final_list_dump

      #Reconciliation Tabular Report
      def Reconciliation_tabular_report(self,from_date,to_date,status_name,supplier_name,bu,show_null_status):
          #custome query=select reconciliation_id,supplier_code,supplier_name,status,changedby_username,changedat,tml_snapshot_vendoroutstandingbalancetotal,supplier_openitem_basicamount,supplier_openitem_others,tml_openitem_gst,tml_openitem_tds,supplier_snapshot_vendoroutstandingbalancetotal,supplier_openitem_gst,supplier_snapshot_tdstotal,supplier_snapshot_uploadedby_username,supplier_snapshot_uploadedat,business_unit from flat_mfg_cv_reconciliation_transitions where business_unit in('CVBU') and supplier_name in('FLEETGUARD FILTERS PVT. LTD.') and status in('PENDING') and changedat>='2023-01-01' and changedat<='2023-04-25' and(reconciliation_id='searchtext' or supplier_code ='searchtext' or supplier_name='searchtext' or status='searchtext' or changedby_username ='searchtext' or changedat ='searchtext' or tml_snapshot_vendoroutstandingbalancetotal ='searchtext' or supplier_openitem_basicamount ='searchtext' or supplier_openitem_others='searchtext' or tml_openitem_gst ='searchtext' or tml_openitem_tds ='searchtext' or supplier_snapshot_vendoroutstandingbalancetotal ='searchtext' or supplier_openitem_basicamount ='searchtext' or supplier_openitem_gst ='searchtext' or supplier_snapshot_tdstotal ='searchtext' or supplier_snapshot_uploadedby_username ='searchtext' or supplier_snapshot_uploadedat ='searchtext' or business_unit ='searchtext')
          #name=index_name(self)
          #es = Elasticsearch(["http://elastic:elastic@172.31.154.203:9200/"])
          
          primary_condations=[]
          primary_condations.append({
            "terms" : {
              "supplier_name.keyword" : supplier_name,
              "boost" : 1.0
            }
          })

          if not status_name:
            print("status_name is empty")
          else:
            if show_null_status == True:
              primary_condations.append(
                {
                  "bool" : {
                    "should" : [
                      {
                        "terms" : {
                          "status.keyword" : status_name,
                          "boost" : 1.0
                        }
                      },
                      {
                        "bool" : {
                          "must_not" : [
                            {
                              "exists" : {
                                "field" : "status",
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
                }
              )
            else:
              primary_condations.append({
               "terms" : {
                 "status.keyword" : status_name,
                 "boost" : 1.0
               }
              })

          # primary_condations.append({
          #  "terms" : {
          #    "status.keyword" : status_name,
          #    "boost" : 1.0
          #  }
          # })
          
          additional_condations=[]
          additional_condations.append({
            "terms" : {
              "business_unit.keyword" : bu
            }
          })
          
          additional_condations.append({
            "range" : {
              "tml_snapshot_asondate" : {
                "from" : from_date,
                "to" : to_date,
                "include_lower" : True,
                "include_upper" : True,
                "time_zone" : "Z",
                "boost" : 1.0,
                "format":"yyyy-MM"
              }
            }
          })

          additional_condations.append({
                      "term" : {
                        "flag" : {
                          "value" : "1",
                          "boost" : 1.0
                        }
                      }
                    })

          count_query={
            "size" : 0,
            "query" : {
              "bool" : {
                "must" : [
                  {
                    "bool" : {
                      "must" : primary_condations,
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
                      "must" : primary_condations,
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
            "_source" : {
              "includes" : [
          "reconciliation_id",
          "business_unit",
          "status" ,
          "changedat",
          "supplier_code",
          "supplier_name",
          "tml_snapshot_id",
          "tml_snapshot_asondate",
          "tml_snapshot_processingtime",
          "tml_snapshot_vendoroutstandingbalancetotal",
          "tml_snapshot_basicamounttotal",
          "tml_snapshot_gsttotal",
          "tml_snapshot_tdstotal",
          "tml_snapshot_otherstotal",
          "tml_snapshot_advancetotal",
          "supplier_snapshot_id",
          "supplier_snapshot_vendoroutstandingbalancetotal",
          "supplier_snapshot_basicamounttotal",
          "supplier_snapshot_gsttotal",
          "supplier_snapshot_tdstotal",
          "supplier_snapshot_otherstotal",
          "supplier_snapshot_advancetotal",
          "supplier_snapshot_differencestats_matches",
          "supplier_snapshot_differencestats_mismatches",
          "supplier_snapshot_differencestats_notfoundtml",
          "supplier_snapshot_differencestats_notfoundsupplier",
          "supplier_snapshot_uploadedat",
          "supplier_snapshot_uploadedby_username",
          "supplier_snapshot_uploadedby_name",
          "changedby_username",
          "changedby_name",
          "id",
          "tml_openitem_id",
          "tml_openitem_companycode",
          "tml_openitem_invoicenumber",
          "tml_openitem_documentnumber",
          "tml_openitem_year",
          "tml_openitem_invoicedate",
          "tml_openitem_postingdate_ba1",
          "tml_openitem_type",
          "tml_openitem_documenttypedescription",
          "tml_openitem_vendoroutstandingbalance",
          "tml_openitem_basicamount",
          "tml_openitem_gst",
          "tml_openitem_tds",
          "tml_openitem_others",
          "supplier_openitem_id",
          "supplier_openitem_invoicenumber",
          "supplier_openitem_invoicedate",
          "supplier_openitem_ponumber",
          "supplier_openitem_grnnumber",
          "supplier_openitem_vendoroutstandingbalance",
          "supplier_openitem_basicamount",
          "supplier_openitem_gst",
          "supplier_openitem_others",
          "category",
          "suppliercomment_comment",
          "suppliercomment_commentedby_username",
          "suppliercomment_commentedby_name",
          "suppliercomment_commentedat",
          "forceclose_id",
          "forceclose_remark_id",
          "forceclose_remark_message",
          "forceclose_remark_actingagency",
          "forceclose_forceclosedby_username",
          "forceclose_forceclosedby_name",
          "forceclose_forceclosedat",
          "basicAmount",
          "vendorOutstandingBalance",
          "tds",
          "diffrence"
          ],
              "excludes" : [ ]
              },
              "docvalue_fields" : [
                      {
                      "field" : "changedat",
                      "format" : "yyyy-MM-dd HH:mm:ss"
                      },
                      {
                      "field" : "supplier_snapshot_uploadedat",
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
          count_df=es.search(index=name,body=count_query,request_timeout=120)
          count=count_df['hits']['total']['value']
          res_vin=es.search(index=name,body=query,size=count,request_timeout=120)
          x2=res_vin['hits']['hits']
          print('x2',x2)
          list=[]
          for i in x2:
              del i['_index']
              del i['_type']
              del i['_id']
              del i['_score']
              dict=i['_source']
              list.append(dict.copy())
          return list
      
            #Reconciliation Tabular Report EXCEL Export
      def Reconciliation_tabular_report_export(self,from_date,to_date,status_name,supplier_name,bu,show_null_status):
          #custome query=select reconciliation_id,supplier_code,supplier_name,status,changedby_username,changedat,tml_snapshot_vendoroutstandingbalancetotal,supplier_openitem_basicamount,supplier_openitem_others,tml_openitem_gst,tml_openitem_tds,supplier_snapshot_vendoroutstandingbalancetotal,supplier_openitem_gst,supplier_snapshot_tdstotal,supplier_snapshot_uploadedby_username,supplier_snapshot_uploadedat,business_unit from flat_mfg_cv_reconciliation_transitions where business_unit in('CVBU') and supplier_name in('FLEETGUARD FILTERS PVT. LTD.') and status in('PENDING') and changedat>='2023-01-01' and changedat<='2023-04-25' and(reconciliation_id='searchtext' or supplier_code ='searchtext' or supplier_name='searchtext' or status='searchtext' or changedby_username ='searchtext' or changedat ='searchtext' or tml_snapshot_vendoroutstandingbalancetotal ='searchtext' or supplier_openitem_basicamount ='searchtext' or supplier_openitem_others='searchtext' or tml_openitem_gst ='searchtext' or tml_openitem_tds ='searchtext' or supplier_snapshot_vendoroutstandingbalancetotal ='searchtext' or supplier_openitem_basicamount ='searchtext' or supplier_openitem_gst ='searchtext' or supplier_snapshot_tdstotal ='searchtext' or supplier_snapshot_uploadedby_username ='searchtext' or supplier_snapshot_uploadedat ='searchtext' or business_unit ='searchtext')
          #name=index_name(self)
          #es = Elasticsearch(["http://elastic:elastic@172.31.154.203:9200/"])
          
          primary_condations=[]
          primary_condations.append({
            "terms" : {
              "supplier_name.keyword" : supplier_name,
              "boost" : 1.0
            }
          })

          # primary_condations.append({
          #  "terms" : {
          #    "status.keyword" : status_name,
          #    "boost" : 1.0
          #  }
          # })

          if not status_name:
            print("status_name is empty")
          else:
            if show_null_status == "true":
              primary_condations.append(
                {
                  "bool" : {
                    "should" : [
                      {
                        "terms" : {
                          "status.keyword" : status_name,
                          "boost" : 1.0
                        }
                      },
                      {
                        "bool" : {
                          "must_not" : [
                            {
                              "exists" : {
                                "field" : "status",
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
                }
              )
            else:
              primary_condations.append({
               "terms" : {
                 "status.keyword" : status_name,
                 "boost" : 1.0
               }
              })
          
          additional_condations=[]
          additional_condations.append({
            "terms" : {
              "business_unit.keyword" : bu
            }
          })
          
          additional_condations.append({
            "range" : {
              "tml_snapshot_asondate" : {
                "from" : from_date,
                "to" : to_date,
                "include_lower" : True,
                "include_upper" : True,
                "time_zone" : "Z",
                "boost" : 1.0,
                "format":"yyyy-MM"
              }
            }
          })
          additional_condations.append({
                      "term" : {
                        "flag" : {
                          "value" : "1",
                          "boost" : 1.0
                        }
                      }
                    })
          count_query={
            "size" : 0,
            "query" : {
              "bool" : {
                "must" : [
                  {
                    "bool" : {
                      "must" : primary_condations,
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
                      "must" : primary_condations,
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
            "_source" : {
              "includes" : [
          "reconciliation_id",
          "business_unit",
          "status" ,
          "changedat",
          "supplier_code",
          "supplier_name",
          "tml_snapshot_id",
          "tml_snapshot_asondate",
          "tml_snapshot_processingtime",
          "tml_snapshot_vendoroutstandingbalancetotal",
          "tml_snapshot_basicamounttotal",
          "tml_snapshot_gsttotal",
          "tml_snapshot_tdstotal",
          "tml_snapshot_otherstotal",
          "tml_snapshot_advancetotal",
          "supplier_snapshot_id",
          "supplier_snapshot_vendoroutstandingbalancetotal",
          "supplier_snapshot_basicamounttotal",
          "supplier_snapshot_gsttotal",
          "supplier_snapshot_tdstotal",
          "supplier_snapshot_otherstotal",
          "supplier_snapshot_advancetotal",
          "supplier_snapshot_differencestats_matches",
          "supplier_snapshot_differencestats_mismatches",
          "supplier_snapshot_differencestats_notfoundtml",
          "supplier_snapshot_differencestats_notfoundsupplier",
          "supplier_snapshot_uploadedat",
          "supplier_snapshot_uploadedby_username",
          "supplier_snapshot_uploadedby_name",
          "changedby_username",
          "changedby_name",
          "id",
          "tml_openitem_id",
          "tml_openitem_companycode",
          "tml_openitem_invoicenumber",
          "tml_openitem_documentnumber",
          "tml_openitem_year",
          "tml_openitem_invoicedate",
          "tml_openitem_postingdate_ba1",
          "tml_openitem_type",
          "tml_openitem_documenttypedescription",
          "tml_openitem_vendoroutstandingbalance",
          "tml_openitem_basicamount",
          "tml_openitem_gst",
          "tml_openitem_tds",
          "tml_openitem_others",
          "supplier_openitem_id",
          "supplier_openitem_invoicenumber",
          "supplier_openitem_invoicedate",
          "supplier_openitem_ponumber",
          "supplier_openitem_grnnumber",
          "supplier_openitem_vendoroutstandingbalance",
          "supplier_openitem_basicamount",
          "supplier_openitem_gst",
          "supplier_openitem_others",
          "category",
          "suppliercomment_comment",
          "suppliercomment_commentedby_username",
          "suppliercomment_commentedby_name",
          "suppliercomment_commentedat",
          "forceclose_id",
          "forceclose_remark_id",
          "forceclose_remark_message",
          "forceclose_remark_actingagency",
          "forceclose_forceclosedby_username",
          "forceclose_forceclosedby_name",
          "forceclose_forceclosedat",
          "basicAmount",
          "vendorOutstandingBalance",
          "tds",
          "diffrence"
                    ],
              "excludes" : [ ]
              },
              "docvalue_fields" : [
                      {
                      "field" : "changedat",
                      "format" : "yyyy-MM-dd HH:mm:ss"
                      },
                      {
                      "field" : "supplier_snapshot_uploadedat",
                      "format" : "yyyy-MM-dd HH:mm:ss"
                      },
                      {
                      "field" : "tml_snapshot_processingtime",
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
          count_df=es.search(index=name,body=count_query,request_timeout=120)
          count=count_df['hits']['total']['value']
          res_vin=es.search(index=name,body=query,size=count,request_timeout=120)
          x2=res_vin['hits']['hits']
          #print('x2',x2)
          list=[]
          for i in x2:
              del i['_index']
              del i['_type']
              del i['_id']
              del i['_score']
              dict=i['_source']
              list.append(dict.copy())
          for i in list:
            print("tml_snapshot_processingtime",i['tml_snapshot_processingtime'])
            if len(i['changedat']) == 20:
                i['changedat']=datetime.strptime(i['changedat'],'%Y-%m-%dT%H:%M:%SZ')
                if i['tml_snapshot_processingtime'] != None:
                  i['tml_snapshot_processingtime']=datetime.strptime(i['tml_snapshot_processingtime'],'%Y-%m-%dT%H:%M:%SZ')
                else:
                  i['tml_snapshot_processingtime']= i['tml_snapshot_processingtime']
                #i['supplier_snapshot_uploadedat']=datetime.strptime(i['supplier_snapshot_uploadedat'],'%Y-%m-%dT%H:%M:%SZ')
            else:
                i['changedat']=datetime.strptime(i['changedat'],'%Y-%m-%dT%H:%M:%S.%fZ')
                if i['tml_snapshot_processingtime'] != None:
                  i['tml_snapshot_processingtime']= datetime.strptime(i['tml_snapshot_processingtime'],'%Y-%m-%dT%H:%M:%S.%fZ')
                else:
                  i['tml_snapshot_processingtime']= i['tml_snapshot_processingtime']
                #i['supplier_snapshot_uploadedat']=datetime.strptime(i['supplier_snapshot_uploadedat'],'%Y-%m-%dT%H:%M:%S.%fZ')
            i['changedat']=i['changedat'].strftime("%Y-%m-%d %H:%M:%S")
            if i['tml_snapshot_processingtime'] != None:
              i['tml_snapshot_processingtime']=i['tml_snapshot_processingtime'].strftime("%Y-%m-%d %H:%M:%S")
            
            #i['supplier_snapshot_uploadedat']=i['supplier_snapshot_uploadedat'].strftime("%Y-%m-%d %H:%M:%S")
          return list
    
      #Graph 2:
      def Reconciliation_supplier_wise_count(self,from_date,to_date,status_name,supplier_name,bu):
            #name="flat_mfg_cv_reconciliation_status_wise_count_transitions"
            #es = Elasticsearch(["http://elastic:elastic@172.31.154.203:9200/"])
          # query= (select status,sum(tml_snapshot_vendorOutStandingBalanceTotal),count(reconciliationId) from flat_mfg_pv_reconciliation_status_wise_count_transitions where tml_snapshot_asOnDate>='2022-01' and tml_snapshot_asOnDate<='2023-02' and supplier_name in('APOLLO TYRES LTD') and status in('CANCELLED') and businessUnit='CVBU' group by status)

          #query= (select status,sum(CASE WHEN status ='IN_REVIEW' THEN 1 ELSE 0 END)as IN_REVIEW_cnt ,sum(CASE WHEN status ='COMPLETED' THEN 1 ELSE 0 END)as COMPLETED_cnt ,sum(CASE WHEN status ='CANCELLED' THEN 1 ELSE 0 END)as CANCELLED_cnt ,sum(CASE WHEN status ='PENDING' THEN 1 ELSE 0 END)as PENDING_cnt ,count(reconciliationId),count(status) from flat_mfg_pv_reconciliation_status_wise_count_transitions where tml_snapshot_asOnDate>='2022-01' and tml_snapshot_asOnDate<='2023-02' and supplier_name in('APOLLO TYRES LTD') and status in('CANCELLED') and businessUnit='CVBU' group by status)

          #query_sum="SELECT SUM(CASE WHEN status ='INITIATED' THEN tml_snapshot_vendorOutStandingBalanceTotal ELSE 0 END) AS INITIATED_sum,SUM(CASE WHEN status ='IN_REVIEW' THEN supplier_snapshot_vendorOutStandingBalanceTotal ELSE 0 END) AS IN_REVIEW_sum,SUM(CASE WHEN status ='COMPLETED' THEN supplier_snapshot_vendorOutStandingBalanceTotal ELSE 0 END) AS COMPLETED_sum,SUM(CASE WHEN status ='CANCELLED' THEN tml_snapshot_vendorOutStandingBalanceTotal ELSE 0 END) AS CANCELLED_sum,SUM(CASE WHEN status ='OUTDATED' THEN tml_snapshot_vendorOutStandingBalanceTotal ELSE 0 END) AS OUTDATED_sum,SUM(CASE WHEN status ='PENDING' THEN tml_snapshot_vendorOutStandingBalanceTotal ELSE 0 END) AS PENDING_sum FROM flat_mfg_pv_reconciliation_status_wise_count_transitions where tml_snapshot_asOnDate>='2022-01' and tml_snapshot_asOnDate<='2023-02' and supplier_name in('APOLLO TYRES LTD') and status in('CANCELLED') and businessUnit in('CVBU') GROUP BY status"
            primary_condations=[]
            if 'undefined' not in supplier_name:
                primary_condations.append({
                  "terms" : {
                    "supplier_name.keyword" : supplier_name
                  }
                })
            if 'undefined' not in status_name:
                primary_condations.append({
                  "terms" : {
                    "status.keyword" : status_name
                  }
                })
            
            additional_condations=[]
            if 'undefined' not in bu:
                additional_condations.append({
            "terms" : {
              "businessUnit.keyword" : bu
            }
                })
 
            additional_condations.append({
              "range" : {
                "tml_snapshot_asOnDate" : {
                  "from" : from_date,
                  "to" : to_date,
                  "include_lower" : True,
                  "include_upper" : True,
                  "time_zone" : "Z",
                  "boost" : 1.0,
                  "format":"yyyy-MM"
                }
              }
            })
            
            status_sum_agg={}
            for i in supplier_name:
                    status_sum_agg.update({i+"_Receivable" : {
          "sum" : {
            "script" : {
              "source" : "InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.eq(InternalQlScriptUtils.docValue(doc,params.v0),params.v1)) ? InternalQlScriptUtils.docValue(doc,params.v2) : params.v3",
              "lang" : "painless",
              "params" : {
                "v0" : "supplier_name.keyword",
                "v1" : i,
                "v2" : "supplier_snapshot_vendorOutStandingBalanceTotal",
                "v3" : 0
              }
            }
          }
        }})
            
            status_sum_agg.update({"count_id" : {
                      "filter" : {
                        "exists" : {
                          "field" : "reconciliationId",
                          "boost" : 1.0
                        }
                      }
                    }})

            query_sum={
              "size" : 0,
              "query" : {
                "bool" : {
                  "must" : [
                    {
                      "bool" : {
                        "must" : primary_condations,
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
                        "supplier_name" : {
                          "terms" : {
                            "field" : "supplier_name.keyword",
                            "missing_bucket" : True,
                            "order" : "asc"
                          }
                        }
                      }
                    ]
                  },
                  "aggregations" : status_sum_agg
                }
              }
            }
            
            res_vin=es.search(index=name_status_transitions,body=query_sum,request_timeout=120)
            list1=[]
            x1=res_vin['aggregations'].get('groupby').get('buckets')
            print("x1",x1)
            #add status of Receivable and Payable value sum in list1
            for i in x1:
                data={}
                data['status']=i['key']['supplier_name']
                data['count']=i['count_id']['doc_count']
                #data['sum']=i['value']
                data['sum'] = i.get(data['status']+"_Receivable", {}).get('value', 0.0) + i.get(data['status']+"_Payable", {}).get('value', 0.0)
                list1.append(data)
            print('list1',list1)
            list1 = [{'status': x['status'], 'count': x['count'], 'sum': round(float(abs(x['sum'])),2)} if x['sum'] < 0 
         else {'status': x['status'], 'count': x['count'], 'sum': round(float(x['sum']),2)} for x in list1]
            #add  in final_list
            final_list = []
            for d in list1:
                status = d['status']
                new_dict = {"supplier_name": status, "count": d["count"], "sum": d["sum"]}
                final_list.append(new_dict)
            return final_list