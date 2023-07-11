"""mes_test_api URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path,include
from . import views

urlpatterns = [
	path('mes_cv/tat/token_api', views.token_api,name="token_api"),
	path('mes_cv/tat/distinct_store_dock', views.distinct_store_dock,name="distinct_store_dock"),
	path('mes_cv/tat/detailedviewlist', views.detailedview,name="detailedview"),
	path('mes_cv/tat/overall_slip_trend', views.overall_slip_trend,name="overall_slip_trend"),
	path('mes_cv/tat/vehicle_turn_around_time', views.vehicle_turn_around_time,name="vehicle_turn_around_time"),
	path('mes_cv/tat/congested_docks', views.congested_docks,name="congested_docks"),
	path('mes_cv/tat/dock_vehicle_turnaround_time', views.dock_vehicle_turnaround_time,name="dock_vehicle_turnaround_time"),
	path("mes_cv/tat/detailedviewexport",views.detailedviewexport,name="detailedviewexportcsv"),
]
