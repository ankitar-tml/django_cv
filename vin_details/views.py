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

from datetime import datetime
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
