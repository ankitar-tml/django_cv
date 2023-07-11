# Create your views here.
from rest_framework.decorators import api_view,authentication_classes,permission_classes
from rest_framework.response import Response
from rest_framework import  permissions
from rest_framework import status
from django.http import HttpResponse
from rest_framework.authentication import SessionAuthentication, BasicAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.authentication import BasicAuthentication, SessionAuthentication,TokenAuthentication
from rest_framework.authtoken.models import Token
from django.contrib.auth.models import User,Group
from . import pullelkdata
from . import serializers
import pandas as pd
import csv
from django.contrib.auth.models import User
from django.db import models
import psycopg2
from datetime import datetime
import datetime

import sys
sys.path.append("cv_api")
import db_connection

db_obj=db_connection.db_con()


obj=pullelkdata.elk_data_extract()

#Token create using POST
@api_view(["POST"])
def token_api(request):
    username = request.data.get("username")
    if username is None:
        return Response({'error': 'Please provide both username and password'},status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    if username != None:
        data = {}
        checkuser = User.objects.filter(username__iexact = username)
        if len(checkuser) != 0:
            data = {}
            try:
                user = User.objects.get(username=username)
                token,_ = Token.objects.get_or_create(user=user)
                #groups = list(user.groups.values_list('name',flat = True))
                #refresh = RefreshToken.for_user(user)
            except User.DoesNotExist:
                raise exceptions.AuthenticationFailed('No such user')
            return Response({'message': token.key,'status':'true'},status=status.HTTP_200_OK)
        else:
            return Response({'message': 'Invalid Credentials','status':'false'},status=status.HTTP_500_INTERNAL_SERVER_ERROR)


#TOP filter 1: distinct plant
@api_view(['POST'])
#@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
#@permission_classes([IsAuthenticated])
def distinct_plant(request):
	serialdata = serializers.distinctSerializer(data = request.data)
	print(request)
	if serialdata.is_valid():
		data = serialdata.data
		plant_id=data["plant_id"]
		data=obj.distinct_plant()
		return Response(data,status=status.HTTP_200_OK)
	else:
		return Response("error",status=status.HTTP_500_INTERNAL_SERVER_ERROR)

#TOP filter 2: distinct shopTypes
@api_view(['POST'])
#@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
#@permission_classes([IsAuthenticated])
def distinct_shopType(request):
	#plant_id=request.GET.get('plant_id')
	serialdata = serializers.distinctSerializer(data = request.data)
	print(request)
	if serialdata.is_valid():
		data = serialdata.data
		data1=obj.distinct_plant()
		dict1={}
		for i in data1:
			dict1[i['plant_name']]=i['plant_id']
		#print(dict1[data["plant_id"]])
		dict1['undefined']='undefined'
		print(dict1)
		plant_id=dict1[data["plant_id"]]
		data=obj.distinct_shopType(plant_id)
		return Response(data,status=status.HTTP_200_OK)
	else:
		return Response("error",status=status.HTTP_500_INTERNAL_SERVER_ERROR)

#TOP filter 3: distinct shop
@api_view(['POST'])
#@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
#@permission_classes([IsAuthenticated])
def distinct_shop(request):
	#plant_id=request.GET.get('plant_id')
	serialdata = serializers.distinctshoptypeSerializer(data = request.data)
	print(request)
	if serialdata.is_valid():
		data = serialdata.data
		data1=obj.distinct_plant()
		dict1={}
		for i in data1:
			dict1[i['plant_name']]=i['plant_id']
		dict1['undefined']='undefined'
		plant_id=dict1[data["plant_id"]]
		shop_type=data["shop_type"]
		data=obj.distinct_shop(plant_id,shop_type)
		return Response(data,status=status.HTTP_200_OK)
	else:
		return Response("error",status=status.HTTP_500_INTERNAL_SERVER_ERROR)

#TOP filter 4: distinct status
@api_view(['POST'])
#@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
#@permission_classes([IsAuthenticated])
def distinct_status(request):
	serialdata = serializers.distinctstatusSerializer(data = request.data)
	print(request)
	if serialdata.is_valid():
		data = serialdata.data
		data1=obj.distinct_plant()
		dict1={}
		for i in data1:
			dict1[i['plant_name']]=i['plant_id']
		dict1['undefined']='undefined'
		plant_id=dict1[data["plant_id"]]
		shop_type=data["shop_type"]
		print(plant_id,shop_type,'shop_type')
		data=obj.distinct_status(plant_id,shop_type)
		for i in data:
			if i['status_name'] == 'REQUIRED':
				i['status_name']="PLANNED"
		return Response(data,status=status.HTTP_200_OK)
	else:
		return Response("error",status=status.HTTP_500_INTERNAL_SERVER_ERROR)

#TOP filter 5: distinct shiftname
@api_view(['POST'])
#@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
#@permission_classes([IsAuthenticated])
def distinct_shiftname(request):
	serialdata = serializers.distinctshiftnameSerializer(data = request.data)
	print(request)
	if serialdata.is_valid():
		data = serialdata.data
		shiftname=data["shiftname"]
		data=obj.distinct_shiftname()
		return Response(data,status=status.HTTP_200_OK)
	else:
		return Response("error",status=status.HTTP_500_INTERNAL_SERVER_ERROR)

#TOP filter 6: distinct Line
@api_view(['POST'])
#authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
#@permission_classes([IsAuthenticated])
def distinct_line(request):
	#plant_id=request.GET.get('plant_id')
	serialdata = serializers.distinctLineSerializer(data = request.data)
	print(request)
	if serialdata.is_valid():
		data = serialdata.data
		data1=obj.distinct_plant()
		dict1={}
		for i in data1:
			dict1[i['plant_name']]=i['plant_id']
		dict1['undefined']='undefined'
		plant_id=dict1[data["plant_id"]]
		shop_type=data["shop_type"]
		shop_type = [s.title() for s in shop_type]
		shop=data["shop"]
		print(plant_id,shop_type,shop)
		data=obj.distinct_line(plant_id,shop_type,shop)
		return Response(data,status=status.HTTP_200_OK)
	else:
		return Response("error",status=status.HTTP_500_INTERNAL_SERVER_ERROR)



#table_view_export
@api_view(['GET'])
#@authentication_classes([BasicAuthentication , SessionAuthentication])
#@permission_classes([IsAuthenticated])
def table_view_export(request):
                plant_id=request.GET.get('plant_id')
                data1=obj.distinct_plant()
                dict1={}
                for i in data1:
                    if i['plant_name'] == plant_id:
                        plant_id = str(i['plant_id'])
                shop_type1=request.GET.get('shop_type')
                shop1=request.GET.get('shop')
                Status1=request.GET.get('Status')
                shift1=request.GET.get('shift')
                line1=request.GET.get('line')
                from_date=request.GET.get('from_date')
                to_date=request.GET.get('to_date')
                shop_type=shop_type1.split(",")
                shop=shop1.split(",")
                Status=Status1.split(",")
                shift=shift1.split(",")
                line=line1.split(",")
                user_name=request.GET.get('username')
                user_id=request.GET.get('user_id')
                report_name=request.GET.get('report_name')
                download_file_type=request.GET.get('download_file_type')
                shop_type_small1=request.GET.get('shop_type_small')
                shop_type_small=shop_type_small1.split(",")
                shop_type_small = [word.capitalize() for word in shop_type_small]
                connection=db_obj.rds_user_data()
                #connection = psycopg2.connect(host = 'mip-cv-appdata.cfz9blub9l7s.ap-south-1.rds.amazonaws.com',port = 5432,user = 'dev_cv', password = 'Dev$#CV',database='manu_cv_dev')
                #connection = psycopg2.connect(host = 'mip-cv-appdata.cfz9blub9l7s.ap-south-1.rds.amazonaws.com',port = 5432,user = 'admin_cv', password = 'Adm!nM@nuCv@123',database='manu_cv')
                #connection = psycopg2.connect(host = 'mip-cv-authdb.cfz9blub9l7s.ap-south-1.rds.amazonaws.com',port = 5432,user = 'admin_cv', password = 'Adm!nM@nuCv@123',database='manufacturing_db')
                cur=connection.cursor()
                from datetime import datetime
                now = datetime.today()
                today_date = now.strftime("%Y-%m-%d")
                today_timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
                #get count
                count_query="""select * from tran_download_logs where user_id='{0}' and currentdate='{1}' and report_name='{2}' and download_file_type='{3}'""".format(user_id,today_date,report_name,download_file_type)
                cur.execute(count_query)
                myresult = cur.fetchall()
                if len(myresult) > 0:
                    count=myresult[0][5]
                    count=count+1
                else:
                    pass

                if len(myresult) > 0:
                    #update record query
                    update_query = """UPDATE tran_download_logs SET download_file_count ='{0}',date_updated='{1}' WHERE user_id = '{2}' and user_name = '{3}' and download_file_type = '{4}' and report_name = '{5}' and  currentdate = '{6}'""".format(count,today_timestamp,user_id,user_name,download_file_type,report_name,today_date)
                    cur.execute(update_query)
                    connection.commit()
                    connection.close()
                    print("Data Updated")
                else:
                    #insert new record
                    download_file_count=1
                    insert_query="""INSERT INTO tran_download_logs(user_id,user_name,currentdate,report_name,download_file_count,download_file_type,date_created,date_updated) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}')""".format(user_id,user_name,today_date,report_name,download_file_count,download_file_type,today_date,today_timestamp)
                    cur.execute(insert_query)
                    connection.commit()
                    connection.close()
                    print("Data Inserted")
                response = HttpResponse(content_type='text/csv')
                response['Content-Disposition'] = 'attachment; filename="plan_vs_actual.csv"'
                excel_data = obj.table_view_export(plant_id,shop_type,shop,Status,shift,line,from_date,to_date)
                data1=obj.distinct_plant()
                for j in excel_data:
                    for i in data1:
                        if str(j['plant'])==str(i['plant_id']):
                            j['plant']=i['plant_name']
                data2=obj.distinct_shop(plant_id,shop_type_small)
                for j in excel_data:
                    for i in data2:
                        if str(j['shop'])==str(i['value']):
                            j['shop']=i['label']
                for i in excel_data:
                    if i['status'] == 'REQUIRED':
                        i['status']="PLANNED"
                #columns=['plant','type','shop','status','materialNumber','asnId','serialNumber','shiftName','modifiedAt']
                columns=['type','shop','status','materialNumber','description','serialNumber','shiftName','modifiedAt']
                df=pd.DataFrame(excel_data,columns=columns)
                print('excel_data',type(excel_data))
                #df['asnId']=''
                #cols = ["Plant Name","Shop Type","Shop","Status","VC","VC Description","Serial Number","Shift","Date"]
                cols = ["Shop Type","Shop","Status","VC","VC Description","Serial Number","Shift","Date"]
                df.columns = cols
                writer = csv.writer(response)
                if df.empty:
                        print("df is empty")
                        writer.writerow(cols)
                else:
                        writer.writerow(df.columns)
                        ddata=df.values.tolist()
                        for row in ddata:
                                writer.writerow(row)
                return response

#Graph 1:
@api_view(['POST'])
@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
@permission_classes([IsAuthenticated])
def plan_vs_actual_daily_count(request):
	serialdata = serializers.tableviewSerializer(data = request.data)
	print(request)
	if serialdata.is_valid():
		data = serialdata.data
		data1=obj.distinct_plant()
		dict1={}
		for i in data1:
			dict1[i['plant_name']]=i['plant_id']
		dict1['undefined']='undefined'
		plant_id=str(dict1[data["plant_id"]])
		shop_type=data["shop_type"]
		shop=data["shop"]
		Status=data["Status"]
		shift=data["shift"]
		line=data["line"]
		from_date=data['from_date']
		to_date=data['to_date']
		data=obj.plan_vs_actual_daily_count(plant_id,shop_type,shop,Status,shift,line,from_date,to_date)
		return Response(data,status=status.HTTP_200_OK)
	else:
		return Response("error",status=status.HTTP_500_INTERNAL_SERVER_ERROR)

#graph 2:
@api_view(['POST'])
@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
@permission_classes([IsAuthenticated])
def plan_vs_actual_hr_month_yr_count(request):
	serialdata = serializers.hr_month_yr_Serializer(data = request.data)
	print(request)
	if serialdata.is_valid():
		data = serialdata.data
		data1=obj.distinct_plant()
		dict1={}
		for i in data1:
			dict1[i['plant_name']]=i['plant_id']
		dict1['undefined']='undefined'
		plant_id=str(dict1[data["plant_id"]])
		shop_type=data["shop_type"]
		shop=data["shop"]
		Status=data["Status"]
		shift=data["shift"]
		line=data["line"]
		duration=data["duration"]
		req_duration=data["duration"]
		from_date=data['from_date']
		to_date=data['to_date']
		data=obj.plan_vs_actual_hr_month_yr_count(plant_id,shop_type,shop,Status,shift,line,duration,req_duration,from_date,to_date)
		return Response(data,status=status.HTTP_200_OK)
	else:
		return Response("error",status=status.HTTP_500_INTERNAL_SERVER_ERROR)

#custome Table View
@api_view(['POST'])
@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
@permission_classes([IsAuthenticated])
def table_view(request):
	serialdata = serializers.CustometableviewSerializer(data = request.data)
	if serialdata.is_valid():
		data = serialdata.data
		data1=obj.distinct_plant()
		dict1={}
		for i in data1:
			dict1[i['plant_name']]=i['plant_id']
		dict1['undefined']='undefined'
		plant_id=str(dict1[data["plant_id"]])
		shop_type=data["shop_type"]
		shop_type_small=data["shop_type_small"]
		shop_type_small = [word.capitalize() for word in shop_type_small]
		shop=data["shop"]
		Status=data["Status"]
		shift=data["shift"]
		line=data["line"]
		from_date=data['from_date']
		to_date=data['to_date']
		searchtext=data["searchtext"]
		date_format='%Y-%m-%d %H:%M:%S'
		if len(searchtext) == 19:
			if str((datetime.datetime.strptime(searchtext, date_format))) == searchtext :
				searchtext=searchtext.replace(' ','T')
		data1=obj.distinct_plant()
		data2=obj.distinct_shop(plant_id,shop_type_small)
		for i in data2:
			if i['label']==searchtext:
				searchtext=str(i['value'])
		for i in data1:
			if i['plant_name']==searchtext:
				searchtext=str(i['plant_id'])
		if searchtext == "PLANNED":
			searchtext="REQUIRED"
		start=data["start"]
		colorder=data['colorder']
		coltype=data['coltype']
		#start=(start-1)*(10)
		size=data['size']
		start=(start-1)*(size)
		data=obj.table_view(plant_id,shop_type,shop,Status,shift,line,from_date,to_date,searchtext,start,colorder,coltype,size,shop_type_small)
		plant_dict={}
		for i in data1:
			plant_dict[i['plant_id']]= i['plant_name']
		for i in data['result']:
			for j in plant_dict:
				if str(i['plant']) == str(j):
					i['plant']=plant_dict.get(j)
		shop_dict={}
		for i in data2:
			shop_dict[i['value']]= i['label']
		for i in data['result']:
			for j in shop_dict:
				if str(i['shop']) == str(j):
					i['shop']=shop_dict.get(j)
		for i in data['result']:
			if i['status'] == 'REQUIRED':
				i['status']="PLANNED"
		return Response(data,status=status.HTTP_200_OK)
	else:
		return Response("error",status=status.HTTP_500_INTERNAL_SERVER_ERROR)