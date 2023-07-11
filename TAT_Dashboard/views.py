from django.shortcuts import render
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
import psycopg2
from datetime import datetime

import sys
sys.path.append("cv_api")
import db_connection

db_obj=db_connection.db_con()

obj=pullelkdata.elk_data_extract()

#Token create using GET
'''@api_view(["GET"])
def token_api(request):
    username = request.GET.get("username")
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
            return Response({'token': token.key},status=status.HTTP_200_OK)
        else:
            return Response({'error': 'Invalid Credentials'},status=status.HTTP_500_INTERNAL_SERVER_ERROR)'''

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

#store and dock drop down list
@api_view(['GET'])
#@authentication_classes([BasicAuthentication , SessionAuthentication])
@permission_classes([IsAuthenticated])
@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
def distinct_store_dock(request):
		data=obj.distinct_store_dock()
		return Response(data,status=status.HTTP_200_OK)

#detailed view list
@api_view(['POST'])
#@authentication_classes([BasicAuthentication , SessionAuthentication])
@permission_classes([IsAuthenticated])
@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
def detailedview(request):
	serialdata = serializers.CountSerializer(data = request.data)
	print(request)
	if serialdata.is_valid():
		data = serialdata.data
		dock_name=data["dock_name"]
		store_name=data["store_name"]
		from_date=data["from_date"]
		to_date=data["to_date"]
		data=obj.detailedview(dock_name,store_name,from_date,to_date)
		return Response(data,status=status.HTTP_200_OK)
	else:
		#print(serialdata.error)
		return Response("error",status=status.HTTP_500_INTERNAL_SERVER_ERROR)

#Graph 1: Overall_slip_gendrated_trend graph
@api_view(['POST'])
#@authentication_classes([BasicAuthentication , SessionAuthentication])
@permission_classes([IsAuthenticated])
@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
def overall_slip_trend(request):
	serialdata = serializers.CountSerializer(data = request.data)
	print(request)
	if serialdata.is_valid():
		data = serialdata.data
		dock_name=data["dock_name"]
		store_name=data["store_name"]
		from_date=data["from_date"]
		to_date=data["to_date"]
		data=obj.overall_slip_trend(dock_name,store_name,from_date,to_date)
		return Response(data,status=status.HTTP_200_OK)
	else:
		#print(serialdata.error)
		return Response("error",status=status.HTTP_500_INTERNAL_SERVER_ERROR)

#graph 2: Top 5 vehicle turn around time
@api_view(['POST'])
#@authentication_classes([BasicAuthentication , SessionAuthentication])
@permission_classes([IsAuthenticated])
@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
def vehicle_turn_around_time(request):
	serialdata = serializers.CountSerializer(data = request.data)
	print(request)
	if serialdata.is_valid():
		data = serialdata.data
		dock_name=data["dock_name"]
		store_name=data["store_name"]
		from_date=data["from_date"]
		to_date=data["to_date"]
		data=obj.vehicle_turn_around_time(dock_name,store_name,from_date,to_date)
		return Response(data,status=status.HTTP_200_OK)
	else:
		#print(serialdata.error)
		return Response("error",status=status.HTTP_500_INTERNAL_SERVER_ERROR)

#Graph 3: Top 5 congested docks		
@api_view(['POST'])
#@authentication_classes([BasicAuthentication , SessionAuthentication])
@permission_classes([IsAuthenticated])
@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
def congested_docks(request):
	serialdata = serializers.CountSerializer(data = request.data)
	print(request)
	if serialdata.is_valid():
		data = serialdata.data
		dock_name=data["dock_name"]
		store_name=data["store_name"]
		from_date=data["from_date"]
		to_date=data["to_date"]
		data=obj.congested_docks(dock_name,store_name,from_date,to_date)
		return Response(data,status=status.HTTP_200_OK)
	else:
		#print(serialdata.error)
		return Response("error",status=status.HTTP_500_INTERNAL_SERVER_ERROR)

#Graph 4:Dock Vehicle Turnaround Time
@api_view(['POST'])
#@authentication_classes([BasicAuthentication , SessionAuthentication])
@permission_classes([IsAuthenticated])
@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
def dock_vehicle_turnaround_time(request):
	serialdata = serializers.CountSerializer(data = request.data)
	print(request)
	if serialdata.is_valid():
		data = serialdata.data
		dock_name=data["dock_name"]
		store_name=data["store_name"]
		from_date=data["from_date"]
		to_date=data["to_date"]
		data=obj.dock_vehicle_turnaround_time(dock_name,store_name,from_date,to_date)
		return Response(data,status=status.HTTP_200_OK)
	else:
		#print(serialdata.error)
		return Response("error",status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
#@authentication_classes([BasicAuthentication , SessionAuthentication])
#@permission_classes([IsAuthenticated])
#@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
def detailedviewexport(request):
                user_name=request.GET.get('username')
                dock_name=request.GET.get('dock_name')
                store_name=request.GET.get('store_name')
                from_date=request.GET.get('from_date')
                to_date=request.GET.get('to_date')
                user_id=request.GET.get('user_id')
                report_name=request.GET.get('report_name')
                download_file_type=request.GET.get('download_file_type')
                #connection = psycopg2.connect(host = 'mip-cv-appdata.cfz9blub9l7s.ap-south-1.rds.amazonaws.com',port = 5432,user = 'dev_cv', password = 'Dev$#CV',database='manu_cv_dev')
                #connection = psycopg2.connect(host = 'mip-cv-appdata.cfz9blub9l7s.ap-south-1.rds.amazonaws.com',port = 5432,user = 'admin_cv', password = 'Adm!nM@nuCv@123',database='manu_cv')
                #connection = psycopg2.connect(host = 'mip-cv-authdb.cfz9blub9l7s.ap-south-1.rds.amazonaws.com',port = 5432,user = 'admin_cv', password = 'Adm!nM@nuCv@123',database='manufacturing_db')
                connection=db_obj.rds_user_data()
                cur=connection.cursor()
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
                response['Content-Disposition'] = 'attachment; filename="TAT.csv"'
                excel_data = obj.detailedviewexport(dock_name,store_name,from_date,to_date)
                columns=['veh_regn_no','slip_no','slip_date','leci_no','vehicle_status','quality_ind','storename','dockname','vs_dockcheckintime','vs_dockcheckouttime','vs_out_in_hrs']
                df=pd.DataFrame(excel_data,columns=columns)
                cols = ["Vehicle/Truck Number","Slip No.","Slip Date","Leci No","Vehicle Status","Quality Indicator","Store Name","Dock Name","Dock Checkin Time","Dock Checkout Time","Dock TAT (in min)"]
                df.columns = cols
                writer = csv.writer(response)
                if df.empty:
                        print("df is empty")
                        writer.writerow(columns)
                else:
                        writer.writerow(df.columns)
                        ddata=df.values.tolist()
                        for row in ddata:
                                writer.writerow(row)
                return response
