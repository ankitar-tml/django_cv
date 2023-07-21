from django.contrib import admin
from django.urls import path,include
from . import views

urlpatterns = [
		path("mes/vin_details/plant=<str:plant>&status=<str:status_name>&from_date=<str:from_date>",views.vin_details,name="vin_details"),
		#path("mes/vin_details/plant=<str:plant>&from_date=<str:from_date>",views.vin_details,name="vin_details")
		#path("api/vin_details",views.vin_details,name="vin_details")
		path("mes/vin_details/part_vin_traceability_search_by_srno",views.vin_traceability_search_by_serial_no,name="vin_details"),
		path("mes/vin_details/part_vin_traceability_search_by_srno_download",views.vin_traceability_search_by_serial_no_download,name="vin_details")
                ]

