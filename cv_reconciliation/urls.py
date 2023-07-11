from django.contrib import admin
from django.urls import path,include
from . import views

urlpatterns = [
    path('mes_cv/cv_reconciliation/token_api', views.token_api,name="token_api"),
	path('mes_cv/cv_reconciliation/distinct_status_supplier', views.distinct_status_supplier,name="distinct_status_supplier"),
    path('mes_cv/cv_reconciliation/distinct_businessUnit', views.distinct_businessUnit,name="distinct_businessUnit"),
    path('mes_cv/cv_reconciliation/Reconciliation_status_wise_count', views.Reconciliation_status_wise_count,name="Reconciliation_status_wise_count"),
    path('mes_cv/cv_reconciliation/Aging_graph', views.Aging_graph,name="Aging_graph"),
    path('mes_cv/cv_reconciliation/Reconciliation_completed_vendors', views.Reconciliation_completed_vendors,name="Reconciliation_completed_vendors"),
    path('mes_cv/cv_reconciliation/Reconciliation_tabular_report', views.Reconciliation_tabular_report,name="Reconciliation_tabular_report"),
    path('mes_cv/cv_reconciliation/Reconciliation_tabular_report_export', views.Reconciliation_tabular_report_export,name="Reconciliation_tabular_report_export"),
    #path('mes_cv/cv_reconciliation/Reconciliation_supplier_wise_count', views.Reconciliation_supplier_wise_count,name="Reconciliation_supplier_wise_count"),
    path('mes_cv/cv_reconciliation/Reconciliation_status_wise_monthly_count', views.Reconciliation_status_wise_monthly_count,name="Reconciliation_status_wise_monthly_count")
]