from django.shortcuts import render
from rest_framework.authentication import BasicAuthentication, SessionAuthentication
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework import status
from . import pullelkdata
from datetime import datetime, timedelta
import time
import datetime
from datetime import datetime, timedelta
from django.core.exceptions import ObjectDoesNotExist
#from vin_details import models
import xlsxwriter
import io
import pandas as pd
from pandasql import sqldf
from datetime import datetime
from django.http import HttpResponse
import pytz
IST = pytz.timezone('Asia/Kolkata') 
datetime_ist = datetime.now(IST)
today=datetime_ist.strftime('%Y-%m-%d %H:%M:%S')
print("today=====:",today)

obj=pullelkdata.HiveDataExtractor()

@api_view(['GET'])
#@authentication_classes([SessionAuthentication, BasicAuthentication])
#@permission_classes([IsAuthenticated])
def vin_details(request,plant,from_date,status_name):
	try:
		plant_code=['1001','1500','2001','3001','3100']
		status_list=['OFFTAKE','undefined']
		if plant not in plant_code:
			data="Please provide proper plant code."
			return Response(data,status=status.HTTP_200_OK)
		elif status_name not in status_list:
			data="Please provide status name either OFFTAKE or Undefined only."
			return Response(data,status=status.HTTP_200_OK)
		else:
			try:
				import datetime
				#y=datetime. datetime. strptime(from_date, '%Y-%m-%d %H:%M:%S')
				from datetime import datetime
				#ts = time.time()
				from datetime import datetime, timedelta
				today = datetime.today()
				today1 = today.strftime("%y-%m-%d")
				yesterday = today - timedelta(days=1)
				date2=yesterday.strftime("%y-%m-%d")
				from datetime import datetime
				import pytz
				IST = pytz.timezone('Asia/Kolkata')
				datetime_ist = datetime.now(IST)
				today1=datetime_ist.strftime('%Y-%m-%d')
				#if date2==from_date[2:10] or today1==from_date[2:10]:
				#if today1==from_date[2:10]:
				if today1==from_date[0:10]:
					#ts = time.time()
					from datetime import datetime, timedelta
					tdate=datetime.today()
					N=0
					IST = pytz.timezone('Asia/Kolkata')
					datetime_ist = datetime.now(IST)
					'''date_N_days_ago = datetime.now() - timedelta(days=N)
					date_N_days_ago = date_N_days_ago + timedelta(minutes=330)
					to_date=date_N_days_ago.strftime('%Y-%m-%d %H:%M:%S')'''
					date_N_days_ago = datetime_ist - timedelta(days=N)
					to_date=date_N_days_ago.strftime('%Y-%m-%d %H:%M:%S')
					data1=obj.elkquery2(plant,from_date,to_date,status_name)
					for i in data1:
						if i['crtd_time'] is not None:
							i['crtd_time']=i['crtd_time'][0:10]+" "+i['crtd_time'][11:19]
						if i['roll_time'] is not None:
							i['roll_time']=i['roll_time'][0:10]+" "+i['roll_time'][11:19]
						if i['wtcl_time'] is not None:
							i['wtcl_time']=i['wtcl_time'][0:10]+" "+i['wtcl_time'][11:19]
						if i['sflt_time'] is not None:
							i['sflt_time']=i['sflt_time'][0:10]+" "+i['sflt_time'][11:19]
						if 'offtake_date' in i.keys():
							if i['offtake_date'] is not None:
								i['offtake_date']=i['offtake_date'][0:10]+" "+i['offtake_date'][11:19]
					listj=[]
					dict1={}
					for i in data1:
						dict1["plant"]=i["plant"]
						dict1["vin_number"]=i["vin_number"]
						dict1["vc_number"]=i["vc_number"]
						dict1["model"]=i["model"]
						dict1["description"]=i["description"]
						dict1["category"]=i["category"]
						dict1["crtd_time"]=i['crtd_time']
						dict1["roll_time"]=i["roll_time"]
						dict1["wtcl_time"]=i["wtcl_time"]
						dict1["sflt_time"]=i["sflt_time"]
						if 'offtake_date' in i.keys():
							dict1["offtake_date"]=i["offtake_date"]
						else:
							dict1["offtake_date"]=None
						dict1["status"]=i["status"]
						listj.append(dict1.copy())
					return Response(listj,status=status.HTTP_200_OK)
				else:
					data1="From Date Should Be Current Date Only."
					return Response(data1,status=status.HTTP_200_OK)
			except Exception as e:
				print(e)
				data="Please provide proper date format in YYYY-MM-DD HH:MM:SS."
				return Response(data,status=status.HTTP_200_OK)
	except Exception as e:
		print(e)
		data="Please provide proper date."
		return Response(data,status=status.HTTP_500_INTERNAL_SERVER_ERROR)
	
@api_view(['POST'])
def vin_traceability_search_by_serial_no(request):
	try:
		get_resp_from_part_genealogy = obj.get_part_genealogy(request.data)
		vin_details = []
		genealogy_details = {}
		if(len(get_resp_from_part_genealogy) > 0):
			for res_obj in get_resp_from_part_genealogy:
				gen_part_obj = {}

				if(res_obj['part_number'] and type(res_obj['part_number']) is list and len(res_obj['part_number']) > 0):
					gen_part_obj["part_number"] = res_obj["part_number"][0]

				if(res_obj['serial_number'] and type(res_obj['serial_number']) is list and len(res_obj['serial_number']) > 0):
					gen_part_obj["serial_number"] = res_obj["serial_number"][0]

				if(res_obj['vin'] and type(res_obj['vin']) is list and len(res_obj['vin']) > 0):
					vin_number = res_obj["vin"][0]
					vin_details.append(vin_number)
					gen_part_obj["vin_number"] = res_obj["vin"][0]
					genealogy_details[f"{vin_number}"] = gen_part_obj
			# print("vin_details",vin_details)
			# print("genealogy_details",genealogy_details)
			params = request.data
			params['vin_number'] = vin_details
			params['genealogy_details'] = genealogy_details
			get_vin_details = obj.get_vin_details(params)
			# print("get_vin_details",get_vin_details)
			return Response(get_vin_details,status=status.HTTP_200_OK)
		else:
			return Response({"result":[]},status=status.HTTP_200_OK)		
	except Exception as e:
		print(e)
		return Response({"result":[]},status=status.HTTP_500_INTERNAL_SERVER_ERROR)
	

@api_view(['GET'])
def vin_traceability_search_by_serial_no_download(request):
	try:
		customColumnDict = {
			"shop":"Shop",
			"vc_number":"VC Number",
			"roll_time":"Booking",
			"line_no":"Line Number",
			"crtd_time":"Drop",
			"sflt_time":"SFLT",
			"description":"Description",
			"model":"Model",
			"wtcl_time":"WTCL",
			"plant":"Plant",
			"vin_number":"VIN Number",
			"part_number":"Part Number",
			"serial_number":"Serial Number"
		}
		response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
		response['Content-Disposition'] = 'attachment; filename="VIN_details.xlsx"'
		newParams = {}
		if ("serial_number" in request.GET.keys()):
			newParams["serial_number"] = request.GET.get("serial_number").split(",")

		if ("part_number" in request.GET.keys()):
			newParams["part_number"] = request.GET.get("part_number").split(",")

		if ("range" in request.GET.keys()):
			newParams["range"] = int(request.GET.get("range"))

		if ("searchtext" in request.GET.keys()):
			newParams["searchtext"] = request.GET.get("searchtext")

		# print("newParams",newParams)
		get_resp_from_part_genealogy = obj.get_part_genealogy(newParams)
		vin_details = []
		genealogy_details = {}
		if(len(get_resp_from_part_genealogy) > 0):
			for res_obj in get_resp_from_part_genealogy:
				gen_part_obj = {}

				if(res_obj['part_number'] and type(res_obj['part_number']) is list and len(res_obj['part_number']) > 0):
					gen_part_obj["part_number"] = res_obj["part_number"][0]

				if(res_obj['serial_number'] and type(res_obj['serial_number']) is list and len(res_obj['serial_number']) > 0):
					gen_part_obj["serial_number"] = res_obj["serial_number"][0]

				if(res_obj['vin'] and type(res_obj['vin']) is list and len(res_obj['vin']) > 0):
					vin_number = res_obj["vin"][0]
					vin_details.append(vin_number)
					gen_part_obj["vin_number"] = res_obj["vin"][0]
					genealogy_details[f"{vin_number}"] = gen_part_obj
			# print("vin_details",vin_details)
			# print("genealogy_details",genealogy_details)
			params = newParams
			params['vin_number'] = vin_details
			params['genealogy_details'] = genealogy_details
			get_vin_details = obj.get_vin_details_in_xlsx(params)
			# print("get_vin_details",get_vin_details)
			output = io.BytesIO()
			workbook = xlsxwriter.Workbook(output, {'in_memory': True})
			worksheet = workbook.add_worksheet()
			worksheet.write(0, 0, "Sr. No.")
			sr_no = 1
			for col_idx, col_name in enumerate(get_vin_details.columns):
				if col_name in customColumnDict.keys():
					# print("customColumnDict[col_name]",customColumnDict[col_name],col_name)
					worksheet.write(0, col_idx+1, customColumnDict[col_name])
				else:
					worksheet.write(0, col_idx, col_name)
			for row_idx, row_data in enumerate(get_vin_details.values, start=1):
				worksheet.write(row_idx, 0, sr_no)
				for col_idx, cell_value in enumerate(row_data):
					worksheet.write(row_idx, col_idx+1, cell_value)
				sr_no = sr_no + 1
			workbook.close()
			output.seek(0)
			response.write(output.getvalue())
			output.close()
			return response
		else:
			return response		
	except Exception as e:
		print(e)
		return response