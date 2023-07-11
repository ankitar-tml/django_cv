from django.contrib import admin
from django.urls import path,include
from . import views

urlpatterns = [
	path('mes_cv/cv_plan_actual/token_api', views.token_api,name="token_api"),
	path('mes_cv/cv_plan_actual/distinct_plant', views.distinct_plant,name="distinct_plant"),
	path('mes_cv/cv_plan_actual/distinct_shopType', views.distinct_shopType,name="distinct_shopType"),
	path('mes_cv/cv_plan_actual/distinct_shop', views.distinct_shop,name="distinct_shop"),
	path('mes_cv/cv_plan_actual/distinct_status', views.distinct_status,name="distinct_status"),
	path('mes_cv/cv_plan_actual/distinct_shiftname', views.distinct_shiftname,name="distinct_shiftname"),
    path('mes_cv/cv_plan_actual/distinct_line', views.distinct_line,name="distinct_line"),
	path('mes_cv/cv_plan_actual/table_view', views.table_view,name="table_view"),
	path('mes_cv/cv_plan_actual/plan_vs_actual_daily_count', views.plan_vs_actual_daily_count,name="plan_vs_actual_daily_count"),
	path('mes_cv/cv_plan_actual/plan_vs_actual_hr_month_yr_count', views.plan_vs_actual_hr_month_yr_count,name="plan_vs_actual_hr_month_yr_count"),
	path('mes_cv/cv_plan_actual/table_view_export',views.table_view_export,name="table_view_export"),
	#path('mes_cv/cv_plan_actual/table_view_custome', views.table_view_custome,name="table_view_custome"),
]
