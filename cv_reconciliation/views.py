
# Create your views here.

from django.shortcuts import render

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
from rest_framework.exceptions import AuthenticationFailed

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
            username = username.lower()
            try:
                user = User.objects.get(username=username)
                token,_ = Token.objects.get_or_create(user=user)
            except User.DoesNotExist:
                raise AuthenticationFailed('No such user')
            return Response({'message': token.key,'status':'true'},status=status.HTTP_200_OK)
        else:
            return Response({'message': 'Invalid Credentials','status':'false'},status=status.HTTP_500_INTERNAL_SERVER_ERROR)

#TOP filter 1: distinct plant
@api_view(['POST'])
#@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication,JWTAuthentication])
#authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
#@permission_classes([IsAuthenticated])
def distinct_status_supplier(request):
	serialdata = serializers.distinct_status_supplier(data = request.data)
	print(request)
	if serialdata.is_valid():
		data = serialdata.data
		status_name=data["status_name"]
		data=obj.distinct_status_supplier()
		return Response(data,status=status.HTTP_200_OK)
	else:
		return Response("error",status=status.HTTP_500_INTERNAL_SERVER_ERROR)

#TOP filter 1: distinct businessUnit
@api_view(['POST'])
#@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication,JWTAuthentication])
@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
@permission_classes([IsAuthenticated])
def distinct_businessUnit(request):
	serialdata = serializers.distinctbusinessUnit(data = request.data)
	print(request)
	if serialdata.is_valid():
		data = serialdata.data
		businessUnit=data["businessUnit"]
		data=obj.distinct_businessUnit()
		return Response(data,status=status.HTTP_200_OK)
	else:
		return Response("error",status=status.HTTP_500_INTERNAL_SERVER_ERROR)

#Graph 1:Reconciliation_status_wise_count
@api_view(['POST'])
#@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication,JWTAuthentication])
@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
@permission_classes([IsAuthenticated])
def Reconciliation_status_wise_count(request):
    serialdata = serializers.Reconciliation_status_wise_count(data = request.data)
    print(request)
    if serialdata.is_valid():
        data = serialdata.data
        from_date=data['from_date']
        to_date=data['to_date']
        status_name=data['status_name']
        supplier_name=data['supplier_name']
        bu=data['bu']
        print(from_date,to_date,status_name,supplier_name,bu)
        data=obj.Reconciliation_status_wise_count(from_date,to_date,status_name,supplier_name,bu)
        return Response(data,status=status.HTTP_200_OK)
    else:
    	return Response("error",status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
#Graph 2:Reconciliation_status_wise_monthly_count
@api_view(['POST'])
#@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication,JWTAuthentication])
@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
@permission_classes([IsAuthenticated])
def Reconciliation_status_wise_monthly_count(request):
    serialdata = serializers.Reconciliation_status_wise_count(data = request.data)
    print(request)
    if serialdata.is_valid():
        data = serialdata.data
        from_date=data['from_date']
        to_date=data['to_date']
        status_name=data['status_name']
        supplier_name=data['supplier_name']
        bu=data['bu']
        print(from_date,to_date,status_name,supplier_name,bu)
        data=obj.Reconciliation_status_wise_monthly_count(from_date,to_date,status_name,supplier_name,bu)
        return Response(data,status=status.HTTP_200_OK)
    else:
    	return Response("error",status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
#Graph 3:Aging_graph
@api_view(['POST'])
#@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication,JWTAuthentication])
@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
@permission_classes([IsAuthenticated])
def Aging_graph(request):
    serialdata = serializers.Reconciliation_status_wise_count(data = request.data)
    print(request)
    if serialdata.is_valid():
        data = serialdata.data
        from_date=data['from_date']
        to_date=data['to_date']
        status_name=data['status_name']
        supplier_name=data['supplier_name']
        bu=data['bu']
        print(from_date,to_date,status_name,supplier_name,bu)
        data=obj.Aging_graph(from_date,to_date,status_name,supplier_name,bu)
        return Response(data,status=status.HTTP_200_OK)
    else:
    	return Response("error",status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
#Graph 4:Reconciliation_completed_vendors
@api_view(['POST'])
#@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication,JWTAuthentication])
@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
@permission_classes([IsAuthenticated])
def Reconciliation_completed_vendors(request):
    serialdata = serializers.Reconciliation_completed_vendors(data = request.data)
    print(request)
    if serialdata.is_valid():
        data = serialdata.data
        supplier_name=data['supplier_name']
        bu=data['bu']
        #status_name=data['status_name']
        from_date=data['from_date']
        to_date=data['to_date']
        #print(from_date,to_date,status_name,supplier_name,bu)
        data=obj.Reconciliation_completed_vendors(bu,supplier_name,from_date,to_date)
        return Response(data,status=status.HTTP_200_OK)
    else:
    	return Response("error",status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
#Reconciliation_tabular_report
@api_view(['POST'])
#@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication,JWTAuthentication])
@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
@permission_classes([IsAuthenticated])
def Reconciliation_tabular_report(request):
    serialdata = serializers.Reconciliation_status_wise_count(data = request.data)
    print(request)
    if serialdata.is_valid():
        data = serialdata.data
        from_date=data['from_date']
        to_date=data['to_date']
        status_name=data['status_name']
        supplier_name=data['supplier_name']
        bu=data['bu']
        show_null_status = data['show_null_status']
        print(from_date,to_date,status_name,supplier_name,bu)
        data=obj.Reconciliation_tabular_report(from_date,to_date,status_name,supplier_name,bu,show_null_status)
        return Response(data,status=status.HTTP_200_OK)
    else:
    	return Response("error",status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
#Reconciliation_tabular_report_export
@api_view(['GET'])
#@authentication_classes([BasicAuthentication , SessionAuthentication])
#@permission_classes([IsAuthenticated])
#@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication,JWTAuthentication])
def Reconciliation_tabular_report_export(request):
                from datetime import datetime
                from_date=request.GET.get('from_date')
                to_date=request.GET.get('to_date')
                status_name1=request.GET.get('status_name')
                status_name=status_name1.split(",")
                supplier_name1=request.GET.get('supplier_name')
                supplier_name=supplier_name1.split(",")
                bu1=request.GET.get('bu')
                bu=bu1.split(",")
                user_name=request.GET.get('username')
                user_id=request.GET.get('user_id')
                report_name=request.GET.get('report_name')
                download_file_type=request.GET.get('download_file_type')
                #connection = psycopg2.connect(host = 'mip-cv-appdata.cfz9blub9l7s.ap-south-1.rds.amazonaws.com',port = 5432,user = 'dev_cv', password = 'Dev$#CV',database='manu_cv_dev')
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
                response['Content-Disposition'] = 'attachment; filename="Reconciliation.csv"'
                #print(from_date,to_date,status_name,supplier_name,bu)
                show_null_status = request.GET.get('show_null_status')
                excel_data = obj.Reconciliation_tabular_report_export(from_date,to_date,status_name,supplier_name,bu,show_null_status)
                #columns=['type','shop','status','materialNumber','asnId','serialNumber','shiftName','modifiedAt']
                columns=["reconciliation_id","business_unit","status" ,"changedat","supplier_code","supplier_name","tml_snapshot_id","tml_snapshot_asondate","tml_snapshot_processingtime","tml_snapshot_vendoroutstandingbalancetotal","tml_snapshot_basicamounttotal","tml_snapshot_gsttotal","tml_snapshot_tdstotal","tml_snapshot_otherstotal","tml_snapshot_advancetotal","supplier_snapshot_id","supplier_snapshot_vendoroutstandingbalancetotal","supplier_snapshot_basicamounttotal","supplier_snapshot_gsttotal","supplier_snapshot_tdstotal","supplier_snapshot_otherstotal","supplier_snapshot_advancetotal","supplier_snapshot_differencestats_matches","supplier_snapshot_differencestats_mismatches","supplier_snapshot_differencestats_notfoundtml","supplier_snapshot_differencestats_notfoundsupplier","supplier_snapshot_uploadedat","supplier_snapshot_uploadedby_username","supplier_snapshot_uploadedby_name","changedby_username","changedby_name","id","tml_openitem_id","tml_openitem_companycode","tml_openitem_invoicenumber","tml_openitem_documentnumber","tml_openitem_year","tml_openitem_invoicedate","tml_openitem_postingdate_ba1","tml_openitem_type","tml_openitem_documenttypedescription","tml_openitem_vendoroutstandingbalance","tml_openitem_basicamount","tml_openitem_gst","tml_openitem_tds","tml_openitem_others","supplier_openitem_id","supplier_openitem_invoicenumber","supplier_openitem_invoicedate","supplier_openitem_ponumber","supplier_openitem_grnnumber","supplier_openitem_vendoroutstandingbalance","supplier_openitem_basicamount","supplier_openitem_gst","supplier_openitem_others","category","suppliercomment_comment","suppliercomment_commentedby_username","suppliercomment_commentedby_name","suppliercomment_commentedat","forceclose_id","forceclose_remark_id","forceclose_remark_message","forceclose_remark_actingagency","forceclose_forceclosedby_username","forceclose_forceclosedby_name","forceclose_forceclosedat","basicAmount","vendorOutstandingBalance","tds","diffrence"]
                df=pd.DataFrame(excel_data,columns=columns)
                #df=pd.DataFrame(excel_data)
                cols = ['Reconciliation ID','Business Unit','Status' ,'Changed At','Supplier Code','Supplier Name','TML Snapshot Id','TMLSnapshot AsOnDate','TMLSnapshot Processing Time','TMLSnapshot Vendor OutStanding Balance Total','TMLSnapshot BasicAmountTotal','TMLSnapshot Gst Total','TMLSnapshot Tds Total','TMLSnapshot Others Total','TMLSnapshot Advance Total','SupplierSnapshot Id','SupplierSnapshot Vendor OutStanding Balance Total','SupplierSnapshot Basic Amount Total','SupplierSnapshot Gst Total','SupplierSnapshot Tds Total','SupplierSnapshot Others Total','SupplierSnapshot Advance Total','SupplierSnapshot.. Matches','SupplierSnapshot.. MisMatches','SupplierSnapshot.. NotFoundTML','SupplierSnapshot.. NotFoundSupplier','SupplierSnapshot Uploaded At','SupplierSnapshot Uploaded By Name','SupplierSnapshot Uploaded By UserName','Change by username','Change by name','id','TMLOpenItem Id','TMLOpenItem Company Code','TMLOpenItem Invoice Number','TMLOpenItem Document Number','TMLOpenItem Year','tml_openitem_invoicedate','TMLOpenItem Posting Date Balance','TMLOpenItem Type','TMLOpenItem Document Type description','TMLOpenItem Vendor OutStanding Balance','TMLOpenItem Basic Amount','TMLOpenItem Gst','TMLOpenItem Tds','TMLOpenItem Others','SupplierOpenItem Id','SupplierOpenItem Invoice Number','SupplierOpenItem Invoice Date','SupplierOpenItem PO Number','SupplierOpenItem GRN Number','SupplierOpenItem Vendor OutStanding Balance','SupplierOpenItem Basic Amount','SupplierOpenItem Gst','SupplierOpenItem Others','Category','SupplierComments Comments','SupplierComments Commented By UserName','SupplierComments Commented By Name','SupplierComments Commented At','ForceClose Id','ForceClose RemarkId','ForceClose RemarkMessage','ForceClose ActingAgency','ForceClose ForceCloseByUsername','ForceClose ForceCloseByName','ForceClose ForceClosedAt','BasicAmount','Vendor Outstanding Balance','tds','diffrence']
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


##Graph 2:Reconciliation_supplier_wise_count
#@api_view(['POST'])
##@authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication,JWTAuthentication])
##authentication_classes([TokenAuthentication,SessionAuthentication, BasicAuthentication])
##@permission_classes([IsAuthenticated])
#def Reconciliation_supplier_wise_count(request):
#    serialdata = serializers.Reconciliation_status_wise_count(data = request.data)
#    print(request)
#    if serialdata.is_valid():
#        data = serialdata.data
#        from_date=data['from_date']
#        to_date=data['to_date']
#        status_name=data['status_name']
#        supplier_name=data['supplier_name']
#        bu=data['bu']
#        print(from_date,to_date,status_name,supplier_name,bu)
#        data=obj.Reconciliation_supplier_wise_count(from_date,to_date,status_name,supplier_name,bu)
#        return Response(data,status=status.HTTP_200_OK)
#    else:
#    	return Response("error",status=status.HTTP_500_INTERNAL_SERVER_ERROR)
