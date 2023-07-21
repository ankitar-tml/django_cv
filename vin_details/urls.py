from django.contrib import admin
from django.urls import path,include
from . import views

urlpatterns = [
		path("mes/vin_details/plant=<str:plant>&status=<str:status_name>&from_date=<str:from_date>",views.vin_details,name="vin_details"),
		#path("mes/vin_details/plant=<str:plant>&from_date=<str:from_date>",views.vin_details,name="vin_details")
		#path("api/vin_details",views.vin_details,name="vin_details")
                ]

