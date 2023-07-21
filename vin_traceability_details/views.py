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