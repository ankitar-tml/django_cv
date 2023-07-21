from django.contrib import admin
from django.urls import path,include
from . import views

urlpatterns = [
		path("mes/vin_traceability_details/part_vin_traceability_search_by_srno",views.vin_traceability_search_by_serial_no,name="vin_traceability_details"),
		path("mes/vin_traceability_details/part_vin_traceability_search_by_srno_download",views.vin_traceability_search_by_serial_no_download,name="vin_traceability_details")
                ]

