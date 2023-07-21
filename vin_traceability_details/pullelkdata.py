#import prestodb
#import csv
#import sys
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from datetime import date
from elasticsearch import Elasticsearch, helpers
#from datetime import datetime
from datetime import datetime, timedelta
import pandas as pd
from pandasql import sqldf

import sys
sys.path.append("cv_api")
import cv_elk_indices
import db_connection

db_obj=db_connection.db_con()
es=db_obj.elk()

index_obj=cv_elk_indices.elk_indices()
index_name=index_obj.vin_details()

class HiveDataExtractor():

	def get_part_genealogy(self,params = {}):
		plants = []
		serial_number = []
		and_condition = []
		part_number = []
		print("params",params)

		fields = [
					{
					"field" : "agg_equnr"
					},
					{
					"field" : "aggr_code"
					},
					{
					"field" : "aggr_erdat"
					},
					{
					"field" : "aggr_erzet"
					},
					{
					"field" : "aggregate_datetime"
					},
					{
					"field" : "date_time"
					},
					{
					"field" : "description"
					},
					{
					"field" : "flag"
					},
					{
					"field" : "model"
					},
					{
					"field" : "p_erdat"
					},
					{
					"field" : "p_erzet"
					},
					{
					"field" : "part_number"
					},
					{
					"field" : "partfamilyname"
					},
					{
					"field" : "plant"
					},
					{
					"field" : "scan_date_time",
					"format" : "strict_date_optional_time_nanos"
					},
					{
					"field" : "serial_number"
					},
					{
					"field" : "skipreason"
					},
					{
					"field" : "srl_flag"
					},
					{
					"field" : "vc_description"
					},
					{
					"field" : "vc_number"
					},
					{
					"field" : "vendor_code"
					},
					{
					"field" : "vendor_name"
					},
					{
					"field" : "vin"
					}
		]



		if ("plants" in params.keys()):
			plants = params['plants']
			# print("plants",plants)
			and_condition.append({
				"terms" : {
					"plant.keyword" : plants,
					"boost" : 1.0
				}
			})

		if("serial_number" in params.keys() and type(params['serial_number']) is list):
			# serial_number.append(params['serial_number'])

			if(len(params['serial_number']) > 1):
				serial_number = params['serial_number']
			elif(len(params['serial_number']) == 1):
				# print("range",0,params.keys())
				if("range" in params.keys()):
					# print("range",1)
					first_sr_no_key = params['serial_number'][0]
					input_srno = first_sr_no_key
					last_three_digit = input_srno[-3:]
					get_value_except_last_three = input_srno[:len(first_sr_no_key)-3]
					# print("get_value_except_last_three",get_value_except_last_three)
					# last_three_digit = '99'
					# print("last_three_digit",last_three_digit,type(last_three_digit))
					con_last_three_digit = int(last_three_digit)
					# print("con_last_three_digit",con_last_three_digit,type(con_last_three_digit))
					
					for x_sr_range in range(con_last_three_digit,params['range']):
						# if x_sr_range <= 99 and x_sr_range >= 10:
							# print("x_sr_range",get_value_except_last_three+str("{:03d}".format(x_sr_range)))
							serial_number.append(get_value_except_last_three+str("{:03d}".format(x_sr_range)))
						# elif x_sr_range < 10:
							# serial_number.append(get_value_except_last_three+str(x_sr_range))
						# else:
							# serial_number.append(get_value_except_last_three+str(x_sr_range))
				else:
					# print("range",2)
					serial_number.append(params['serial_number'][0])
			
			
			and_condition.append(
				{
					"terms" : {
						"serial_number.keyword" : serial_number,
						"boost" : 1.0
					}
				}
			)
		elif("part_number" in params.keys() and type(params['part_number']) is list):
			if(len(params['part_number']) > 1):
				part_number = params['part_number']
			elif(len(params['part_number']) == 1):
				# print("range",0,params.keys())
				if("range" in params.keys()):
					# print("range",1)
					first_prt_no_key = params['part_number'][0]
					input_srno = first_prt_no_key
					last_three_digit_prt_no = input_srno[-3:]
					get_prt_no_value_except_last_three = input_srno[:len(first_prt_no_key)-3]
					# print("get_prt_no_value_except_last_three",get_prt_no_value_except_last_three)
					# last_three_digit_prt_no = '99'
					# print("last_three_digit_prt_no",last_three_digit_prt_no,type(last_three_digit_prt_no))
					con_last_three_digit_prt_no = int(last_three_digit_prt_no)
					# print("con_last_three_digit_prt_no",con_last_three_digit_prt_no,type(con_last_three_digit_prt_no))
					
					for x_prt_no_sr_range in range(con_last_three_digit_prt_no,params['range']):
						# if x_prt_no_sr_range <= 99 and x_prt_no_sr_range >= 10:
							# print("x_prt_no_sr_range",get_prt_no_value_except_last_three+str("{:03d}".format(x_prt_no_sr_range)))
							part_number.append(get_prt_no_value_except_last_three+str("{:03d}".format(x_prt_no_sr_range)))
						# elif x_prt_no_sr_range < 10:
							# part_number.append(get_prt_no_value_except_last_three+str(x_prt_no_sr_range))
						# else:
							# part_number.append(get_prt_no_value_except_last_three+str(x_prt_no_sr_range))
				else:
					# print("range",2)
					part_number.append(params['part_number'][0])
			
			
			and_condition.append(
				{
					"terms" : {
						"part_number.keyword" : part_number,
						"boost" : 1.0
					}
				}
			)

		# print("serial_number",serial_number)
		query_obj = {
				"query" : {
					"bool" : {
					"must" : and_condition,
					"adjust_pure_negative" : True,
					"boost" : 1.0
					}
				},
				"_source" : False,
				"fields" : fields,
				"sort" : [
					{
						"date_time.keyword" : {
							"order" : "desc",
							"missing" : "_first",
							"unmapped_type" : "keyword"
						}
					}
				]
		}
		
		# print(query_obj)
		res_part_gen = es.search(index="flat_mfg_part_genealogy",body=query_obj,size=10000,request_timeout=120)
		x2=res_part_gen['hits'].get('hits')
		# print("x2",x2)
		res_list = []
		dict_vin = {}
		for i in x2:
				del i['_type']
				del i['_score']
				del i['_index']
				del i['_id']
				# print(i['fields'])
				dict_vin=i['fields']
				res_list.append(dict_vin.copy())
		return res_list
		# return query_obj

	def get_vin_details(self,params):
		plant_obj = {}
		get_plant_details = self.get_plants_details()
		result_count = 0
		req_size = 10
		req_start = 0
		and_condition = []
		sort_column = "crtd_time"
		sort_type = "desc"

		if "size" in params.keys():
			req_size = int(params["size"])

		if "start" in params.keys():
			req_start = int((params["start"]-1)*req_size)
			
		if "colorder" in params.keys() and params["colorder"] != "" and params["colorder"] != "undefined":
			sort_type = params["colorder"]

		if "colname" in params.keys() and params["colname"] != "" and params["colname"] != "undefined":
			sort_column = params["colname"]

		# print("sort_type",sort_type)
		# print("sort_column",sort_column)
		# print("get_plant_details",get_plant_details)
		# print("datetime_valid",self.datetime_valid(params["searchtext"])) 

		for each_plant in get_plant_details:
			if "plant_name" in each_plant.keys() and "plant_id" in each_plant.keys():
				plant_obj[each_plant["plant_id"]] = each_plant["plant_name"]

		# print("plant_obj",plant_obj)
		
		if("vin_number" in params.keys() and type(params['vin_number']) is list):
			and_condition.append(
				{
					"terms" : {
						"vin_number.keyword" : params['vin_number'],
						"boost" : 1.0
					}
				}
			)

		# if("searchtext" in params.keys() and params["searchtext"] != ""):
		# 	if self.datetime_valid(params["searchtext"]):
		# 		and_condition.append({
		# 			"bool" : {
		# 				"should" : [
		# 				{
		# 					"bool" : {
		# 					"should" : [
		# 						{
		# 						"bool" : {
		# 							"should" : [
		# 							{
		# 								"term" : {
		# 								"roll_time" : {
		# 									"value" : params["searchtext"],
		# 									"boost" : 1.0
		# 								}
		# 								}
		# 							},
		# 							{
		# 								"term" : {
		# 								"crtd_time" : {
		# 									"value" : params["searchtext"],
		# 									"boost" : 1.0
		# 								}
		# 								}
		# 							}
		# 							],
		# 							"adjust_pure_negative" : True,
		# 							"boost" : 1.0
		# 						}
		# 						},
		# 						{
		# 						"term" : {
		# 							"sflt_time" : {
		# 							"value" : params["searchtext"],
		# 							"boost" : 1.0
		# 							}
		# 						}
		# 						}
		# 					],
		# 					"adjust_pure_negative" : True,
		# 					"boost" : 1.0
		# 					}
		# 				},
		# 				{
		# 					"term" : {
		# 					"wtcl_time" : {
		# 						"value" : params["searchtext"],
		# 						"boost" : 1.0
		# 					}
		# 					}
		# 				}
		# 				],
		# 				"adjust_pure_negative" : True,
		# 				"boost" : 1.0
		# 			}
		# 		})
		# 	else:
		# 		and_condition.append({
				# 	"bool" : {
				# 		"should" : [
				# 		{
				# 			"bool" : {
				# 			"should" : [
				# 				{
				# 				"bool" : {
				# 					"should" : [
				# 					{
				# 						"bool" : {
				# 						"should" : [
				# 							{
				# 							"bool" : {
				# 								"should" : [
				# 								{
				# 									"bool" : {
				# 									"should" : [
				# 										{
				# 										"bool" : {
				# 											"should" : [
				# 											{
				# 												"bool" : {
				# 												"should" : [
				# 													{
				# 													"bool" : {
				# 														"should" : [
				# 														{
				# 															"bool" : {
				# 															"should" : [
				# 																{
				# 																"bool" : {
				# 																	"should" : [
				# 																	{
				# 																		"bool" : {
				# 																		"should" : [
				# 																			{
				# 																			"bool" : {
				# 																				"should" : [
				# 																				{
				# 																					"bool" : {
				# 																					"should" : [
				# 																						{
				# 																						"bool" : {
				# 																							"should" : [
				# 																							{
				# 																								"bool" : {
				# 																								"should" : [
				# 																									{
				# 																									"bool" : {
				# 																										"should" : [
				# 																										{
				# 																											"wildcard" : {
				# 																											"shop.keyword" : {
				# 																												"wildcard" : f"*{params['search']}",
				# 																												"boost" : 1.0
				# 																											}
				# 																											}
				# 																										},
				# 																										{
				# 																											"wildcard" : {
				# 																											"shop.keyword" : {
				# 																												"wildcard" : f"{params['search']}*",
				# 																												"boost" : 1.0
				# 																											}
				# 																											}
				# 																										}
				# 																										],
				# 																										"adjust_pure_negative" : True,
				# 																										"boost" : 1.0
				# 																									}
				# 																									},
				# 																									{
				# 																									"wildcard" : {
				# 																										"shop.keyword" : {
				# 																										"wildcard" : f"*{params['search']}*",
				# 																										"boost" : 1.0
				# 																										}
				# 																									}
				# 																									}
				# 																								],
				# 																								"adjust_pure_negative" : True,
				# 																								"boost" : 1.0
				# 																								}
				# 																							},
				# 																							{
				# 																								"wildcard" : {
				# 																								"vc_number.keyword" : {
				# 																									"wildcard" : f"*{params['search']}",
				# 																									"boost" : 1.0
				# 																								}
				# 																								}
				# 																							}
				# 																							],
				# 																							"adjust_pure_negative" : True,
				# 																							"boost" : 1.0
				# 																						}
				# 																						},
				# 																						{
				# 																						"wildcard" : {
				# 																							"vc_number.keyword" : {
				# 																							"wildcard" : f"{params['search']}*",
				# 																							"boost" : 1.0
				# 																							}
				# 																						}
				# 																						}
				# 																					],
				# 																					"adjust_pure_negative" : True,
				# 																					"boost" : 1.0
				# 																					}
				# 																				},
				# 																				{
				# 																					"wildcard" : {
				# 																					"vc_number.keyword" : {
				# 																						"wildcard" : f"*{params['search']}*",
				# 																						"boost" : 1.0
				# 																					}
				# 																					}
				# 																				}
				# 																				],
				# 																				"adjust_pure_negative" : True,
				# 																				"boost" : 1.0
				# 																			}
				# 																			},
				# 																			{
				# 																			"wildcard" : {
				# 																				"description.keyword" : {
				# 																				"wildcard" : f"*{params['search']}",
				# 																				"boost" : 1.0
				# 																				}
				# 																			}
				# 																			}
				# 																		],
				# 																		"adjust_pure_negative" : True,
				# 																		"boost" : 1.0
				# 																		}
				# 																	},
				# 																	{
				# 																		"wildcard" : {
				# 																		"description.keyword" : {
				# 																			"wildcard" : f"{params['search']}*",
				# 																			"boost" : 1.0
				# 																		}
				# 																		}
				# 																	}
				# 																	],
				# 																	"adjust_pure_negative" : True,
				# 																	"boost" : 1.0
				# 																}
				# 																},
				# 																{
				# 																"wildcard" : {
				# 																	"description.keyword" : {
				# 																	"wildcard" : f"*{params['search']}*",
				# 																	"boost" : 1.0
				# 																	}
				# 																}
				# 																}
				# 															],
				# 															"adjust_pure_negative" : True,
				# 															"boost" : 1.0
				# 															}
				# 														},
				# 														{
				# 															"wildcard" : {
				# 															"model.keyword" : {
				# 																"wildcard" : f"*{params['search']}",
				# 																"boost" : 1.0
				# 															}
				# 															}
				# 														}
				# 														],
				# 														"adjust_pure_negative" : True,
				# 														"boost" : 1.0
				# 													}
				# 													},
				# 													{
				# 													"wildcard" : {
				# 														"model.keyword" : {
				# 														"wildcard" : f"{params['search']}*",
				# 														"boost" : 1.0
				# 														}
				# 													}
				# 													}
				# 												],
				# 												"adjust_pure_negative" : True,
				# 												"boost" : 1.0
				# 												}
				# 											},
				# 											{
				# 												"wildcard" : {
				# 												"model.keyword" : {
				# 													"wildcard" : f"*{params['search']}*",
				# 													"boost" : 1.0
				# 												}
				# 												}
				# 											}
				# 											],
				# 											"adjust_pure_negative" : True,
				# 											"boost" : 1.0
				# 										}
				# 										},
				# 										{
				# 										"wildcard" : {
				# 											"plant.keyword" : {
				# 											"wildcard" : f"*{params['search']}",
				# 											"boost" : 1.0
				# 											}
				# 										}
				# 										}
				# 									],
				# 									"adjust_pure_negative" : True,
				# 									"boost" : 1.0
				# 									}
				# 								},
				# 								{
				# 									"wildcard" : {
				# 									"plant.keyword" : {
				# 										"wildcard" : f"{params['search']}*",
				# 										"boost" : 1.0
				# 									}
				# 									}
				# 								}
				# 								],
				# 								"adjust_pure_negative" : True,
				# 								"boost" : 1.0
				# 							}
				# 							},
				# 							{
				# 							"wildcard" : {
				# 								"plant.keyword" : {
				# 								"wildcard" : f"*{params['search']}*",
				# 								"boost" : 1.0
				# 								}
				# 							}
				# 							}
				# 						],
				# 						"adjust_pure_negative" : True,
				# 						"boost" : 1.0
				# 						}
				# 					},
				# 					{
				# 						"wildcard" : {
				# 						"vin_number.keyword" : {
				# 							"wildcard" : f"*{params['search']}",
				# 							"boost" : 1.0
				# 						}
				# 						}
				# 					}
				# 					],
				# 					"adjust_pure_negative" : True,
				# 					"boost" : 1.0
				# 				}
				# 				},
				# 				{
				# 				"wildcard" : {
				# 					"vin_number.keyword" : {
				# 					"wildcard" : f"{params['search']}*",
				# 					"boost" : 1.0
				# 					}
				# 				}
				# 				}
				# 			],
				# 			"adjust_pure_negative" : True,
				# 			"boost" : 1.0
				# 			}
				# 		},
				# 		{
				# 			"wildcard" : {
				# 			"vin_number.keyword" : {
				# 				"wildcard" : f"*{params['search']}*",
				# 				"boost" : 1.0
				# 			}
				# 			}
				# 		}
				# 		],
				# 		"adjust_pure_negative" : True,
				# 		"boost" : 1.0
				# 	}
				# })

		query_obj = {
				# "from":req_start,
				"query" : {
					"bool" : {
					"must" : and_condition,
					"adjust_pure_negative" : True,
					"boost" : 1.0
					}
				},
				"_source" : {
					"includes" : [
						"ag_asn",
						"asnId",
						"category",
						"description",
						"model",
						"shop",
						"status",
						"unique_id",
						"vc_number",
						"crtd_time",
						"roll_time",
						"sflt_time",
						"wtcl_time",
						"plant"
						"vin_number"
				],
				"excludes" : []
				},
				"docvalue_fields" : [
							{
								"field" : "crtd_time",
								"format" : "yyyy-MM-dd HH:mm:ss"
							},
							{
								"field" : "roll_time",
								"format" : "yyyy-MM-dd HH:mm:ss"
							},
							{
								"field" : "sflt_time",
								"format" : "yyyy-MM-dd HH:mm:ss"
							},
							{
								"field" : "wtcl_time",
								"format" : "yyyy-MM-dd HH:mm:ss"
							},
				],
				"fields" : [{"field":"plant"}, {"field":"vin_number"}],
				"sort" : [
					{
						"date_time.keyword" : {
							"order" : "desc",
							"missing" : "_first",
							"unmapped_type" : "keyword"
						}
					}
				]
		}

		res_part_gen = es.search(index="flat_mfg_vin_details",body=query_obj,size=10000,request_timeout=120)
		x2=res_part_gen['hits'].get('hits')
		# print("res_part_gen['hits'].get('hits')",res_part_gen['hits'].get('total'))
		if "total" in res_part_gen['hits'].keys() and  "value" in res_part_gen['hits'].get('total'):
			total_obj = res_part_gen['hits'].get('total')
			result_count = total_obj["value"]
		res_list = []
		dict_vin = {}
		for i in x2:
				del i['_type']
				del i['_score']
				del i['_index']
				del i['_id']
				# print(i)
				dict_vin=i['_source']
				if i["fields"]["plant"] and type(i["fields"]["plant"]) is list and i["fields"]["plant"][0]:
					dict_vin["plant"] = i["fields"]["plant"][0]

				if i["fields"]["vin_number"] and type(i["fields"]["vin_number"]) is list and i["fields"]["vin_number"][0]:
					dict_vin["vin_number"] = i["fields"]["vin_number"][0]

				if "roll_time" in i["fields"].keys() and type(i["fields"]["roll_time"]) is list and  i["fields"]["roll_time"][0] != "" and self.datetime_valid(i["fields"]["roll_time"][0]):
					roll_time_temp = i["fields"]["roll_time"][0]
					dict_vin["roll_time"] = roll_time_temp
					# print("roll_time",roll_time_temp,dict_vin["roll_time"])

				if "crtd_time" in i["fields"].keys() and type(i["fields"]["crtd_time"]) is list and  i["fields"]["crtd_time"][0] != "" and self.datetime_valid(i["fields"]["crtd_time"][0]):
					crtd_time_temp = i["fields"]["crtd_time"][0]
					dict_vin["crtd_time"] = crtd_time_temp
					# print("crtd_time",crtd_time_temp,dict_vin["crtd_time"])

				if "sflt_time" in i["fields"].keys() and type(i["fields"]["sflt_time"]) is list and  i["fields"]["sflt_time"][0] != "" and self.datetime_valid(i["fields"]["sflt_time"][0]):
					sflt_time_temp = i["fields"]["sflt_time"][0]
					dict_vin["sflt_time"] = sflt_time_temp
					# print("sflt_time",sflt_time_temp,dict_vin["sflt_time"])

				if "wtcl_time" in i["fields"].keys() and type(i["fields"]["wtcl_time"]) is list and  i["fields"]["wtcl_time"][0] != "" and self.datetime_valid(i["fields"]["wtcl_time"][0]):
					wtcl_time_temp = i["fields"]["wtcl_time"][0]
					dict_vin["wtcl_time"] = wtcl_time_temp
					# print("wtcl_time",wtcl_time_temp,dict_vin["wtcl_time"])

				if "genealogy_details" in params.keys() and "vin_number" in dict_vin.keys() and dict_vin["vin_number"] in params["genealogy_details"].keys():
					# print("gen_details",params["genealogy_details"][dict_vin["vin_number"]])
					if "part_number" in params["genealogy_details"][dict_vin["vin_number"]].keys():
						dict_vin["part_number"] = params["genealogy_details"][dict_vin["vin_number"]]["part_number"]

					if "serial_number" in params["genealogy_details"][dict_vin["vin_number"]].keys():
						dict_vin["serial_number"] = params["genealogy_details"][dict_vin["vin_number"]]["serial_number"]

				if "plant" in dict_vin.keys() and dict_vin["plant"]:
					plan_key_code = dict_vin["plant"]
					plan_key_code = int(plan_key_code)
					# print("plant_code",type(plan_key_code),plan_key_code,plant_obj[plan_key_code])
					if plant_obj and plant_obj[plan_key_code]:
						dict_vin["plant_name"] = plant_obj[plan_key_code]
					# if dict_vin["plant"][0] in plant_obj.keys() and plant_obj[dict_vin["plant"][0]]["shop.plant.name"] and plant_obj[dict_vin["plant"][0]]["shop.plant.name"][0]:
					# 	print("plant_name",plant_obj[dict_vin["plant"][0]])
					# 	dict_vin["plant_name"] = plant_obj[dict_vin["plant"][0]]["shop.plant.name"][0]
				res_list.append(dict_vin.copy())
		# return {
		# 	"count":result_count,
		# 	"result":res_list,
		# }

		# print(res_list)
		df = pd.DataFrame(res_list)
		df_count = pd.DataFrame(res_list)
		# print(df)
		search_query = ""
		if "searchtext" in params.keys():
			search_query = params["searchtext"]
		# print("search_query", search_query)
		raw_query = f"select * from df where (shop like '%{search_query}%' or shop like '{search_query}%' or shop like '%{search_query}' or vc_number like '%{search_query}%' or vc_number like '{search_query}%' or vc_number like '%{search_query}' or vin_number like '%{search_query}%' or vin_number like '{search_query}%' or vin_number like '%{search_query}' or description like '%{search_query}%' or description like '{search_query}%' or description like '%{search_query}' or model like '%{search_query}%' or model like '{search_query}%' or model like '%{search_query}' or plant_name like '%{search_query}%' or plant_name like '{search_query}%' or plant_name like '%{search_query}' or serial_number like '%{search_query}%' or serial_number like '{search_query}%' or serial_number like '%{search_query}' or part_number like '%{search_query}%' or part_number like '{search_query}%' or part_number like '%{search_query}' or roll_time = '{search_query}' or crtd_time = '{search_query}' or sflt_time = '{search_query}' or wtcl_time = '{search_query}') order by {sort_column} {sort_type} limit {req_start}, {req_size} "
		# print("raw_query",raw_query)
		df = sqldf(raw_query)
		# print("df",df)
		df_dict = df.to_dict('records')
		# print(df_dict)
		
		count_query = f"select count(*) as count from df_count where (shop like '%{search_query}%' or shop like '{search_query}%' or shop like '%{search_query}' or vc_number like '%{search_query}%' or vc_number like '{search_query}%' or vc_number like '%{search_query}' or vin_number like '%{search_query}%' or vin_number like '{search_query}%' or vin_number like '%{search_query}' or description like '%{search_query}%' or description like '{search_query}%' or description like '%{search_query}' or model like '%{search_query}%' or model like '{search_query}%' or model like '%{search_query}' or plant_name like '%{search_query}%' or plant_name like '{search_query}%' or plant_name like '%{search_query}' or serial_number like '%{search_query}%' or serial_number like '{search_query}%' or serial_number like '%{search_query}' or part_number like '%{search_query}%' or part_number like '{search_query}%' or part_number like '%{search_query}' or roll_time = '{search_query}' or crtd_time = '{search_query}' or sflt_time = '{search_query}' or wtcl_time = '{search_query}')"
		count=sqldf(count_query)
		count=count.to_dict('records')
		if type(count) is list and len(count) > 0 and "count" in count[0].keys():
			count = count[0]["count"]
		# count = count.values.tolist()
		# print("count",count)
		return {
			"count":count,
			"result":df_dict
		}
	
	def get_plants_details(self):
		fields = [
					{
					"field" : "shop.plant.city"
					},
					{
					"field" : "shop.plant.country"
					},
					{
					"field" : "shop.plant.id"
					},
					{
					"field" : "shop.plant.ipmsPlantId"
					},
					{
					"field" : "shop.plant.name"
					},
					{
					"field" : "shop.plant.sapPlantId"
					},
					{
					"field" : "shop.plant.state"
					},
					{
					"field" : "shop.type"
					},
					{
					"field" : "type"
					}
		]

		query_obj = {
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

		res_part_gen = es.search(index="flat_mfg_digital_twin_api",body=query_obj,size=10000,request_timeout=120)
		x2=res_part_gen['aggregations'].get('groupby').get('buckets')
		# print("x2",x2)
		res_list = []
		for i in x2:
			data={}
			data['plant_id']=i['key']['plant_id']
			data['plant_name']=i['key']['plant_name']
			res_list.append(data)
		return res_list
	
	def datetime_valid(self,dt_str):
		try:
			datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
		except:
			return False
		return True
	
	def get_vin_details_in_xlsx(self,params):
		plant_obj = {}
		get_plant_details = self.get_plants_details()
		and_condition = []

		for each_plant in get_plant_details:
			if "plant_name" in each_plant.keys() and "plant_id" in each_plant.keys():
				plant_obj[each_plant["plant_id"]] = each_plant["plant_name"]

		# print("plant_obj",plant_obj)
		
		if("vin_number" in params.keys() and type(params['vin_number']) is list):
			and_condition.append(
				{
					"terms" : {
						"vin_number.keyword" : params['vin_number'],
						"boost" : 1.0
					}
				}
			)

		query_obj = {
				# "from":req_start,
				"query" : {
					"bool" : {
					"must" : and_condition,
					"adjust_pure_negative" : True,
					"boost" : 1.0
					}
				},
				"_source" : {
					"includes" : [
						"description",
						"model",
						"shop",
						"vc_number",
						"crtd_time",
						"roll_time",
						"sflt_time",
						"wtcl_time",
						"plant"
						"vin_number"
				],
				"excludes" : []
				},
				"docvalue_fields" : [
							{
								"field" : "crtd_time",
								"format" : "yyyy-MM-dd HH:mm:ss"
							},
							{
								"field" : "roll_time",
								"format" : "yyyy-MM-dd HH:mm:ss"
							},
							{
								"field" : "sflt_time",
								"format" : "yyyy-MM-dd HH:mm:ss"
							},
							{
								"field" : "wtcl_time",
								"format" : "yyyy-MM-dd HH:mm:ss"
							},
				],
				"fields" : [{"field":"plant"}, {"field":"vin_number"}],
				"sort" : [
					{
						"date_time.keyword" : {
							"order" : "desc",
							"missing" : "_first",
							"unmapped_type" : "keyword"
						}
					}
				]
		}

		res_part_gen = es.search(index="flat_mfg_vin_details",body=query_obj,size=10000,request_timeout=120)
		x2=res_part_gen['hits'].get('hits')
		# print("res_part_gen['hits'].get('hits')",res_part_gen['hits'].get('total'))
		if "total" in res_part_gen['hits'].keys() and  "value" in res_part_gen['hits'].get('total'):
			total_obj = res_part_gen['hits'].get('total')
			result_count = total_obj["value"]
		res_list = []
		dict_vin = {}
		for i in x2:
				del i['_type']
				del i['_score']
				del i['_index']
				del i['_id']
				# print(i)
				dict_vin=i['_source']
				if i["fields"]["plant"] and type(i["fields"]["plant"]) is list and i["fields"]["plant"][0]:
					dict_vin["plant"] = i["fields"]["plant"][0]

				if i["fields"]["vin_number"] and type(i["fields"]["vin_number"]) is list and i["fields"]["vin_number"][0]:
					dict_vin["vin_number"] = i["fields"]["vin_number"][0]

				if "genealogy_details" in params.keys() and "vin_number" in dict_vin.keys() and dict_vin["vin_number"] in params["genealogy_details"].keys():
					# print("gen_details",params["genealogy_details"][dict_vin["vin_number"]])
					if "part_number" in params["genealogy_details"][dict_vin["vin_number"]].keys():
						dict_vin["part_number"] = params["genealogy_details"][dict_vin["vin_number"]]["part_number"]

					if "serial_number" in params["genealogy_details"][dict_vin["vin_number"]].keys():
						dict_vin["serial_number"] = params["genealogy_details"][dict_vin["vin_number"]]["serial_number"]

				if "plant" in dict_vin.keys() and dict_vin["plant"]:
					plan_key_code = dict_vin["plant"]
					plan_key_code = int(plan_key_code)
					# print("plant_code",type(plan_key_code),plan_key_code,plant_obj[plan_key_code])
					if plant_obj and plant_obj[plan_key_code]:
						dict_vin["plant"] = plant_obj[plan_key_code]
				res_list.append(dict_vin.copy())

		# print(res_list)
		df = pd.DataFrame(res_list)
		search_query = ""
		if "searchtext" in params.keys():
			search_query = params["searchtext"]
		# print("search_query", search_query)
		raw_query = f"select * from df where (shop like '%{search_query}%' or shop like '{search_query}%' or shop like '%{search_query}' or vc_number like '%{search_query}%' or vc_number like '{search_query}%' or vc_number like '%{search_query}' or vin_number like '%{search_query}%' or vin_number like '{search_query}%' or vin_number like '%{search_query}' or description like '%{search_query}%' or description like '{search_query}%' or description like '%{search_query}' or model like '%{search_query}%' or model like '{search_query}%' or model like '%{search_query}' or plant like '%{search_query}%' or plant like '{search_query}%' or plant like '%{search_query}' or serial_number like '%{search_query}%' or serial_number like '{search_query}%' or serial_number like '%{search_query}' or part_number like '%{search_query}%' or part_number like '{search_query}%' or part_number like '%{search_query}' or roll_time = '{search_query}' or crtd_time = '{search_query}' or sflt_time = '{search_query}' or wtcl_time = '{search_query}')"
		# print("raw_query",raw_query)
		df = sqldf(raw_query)
		return df







		

		
