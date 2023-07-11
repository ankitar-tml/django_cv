from rest_framework import serializers
#from .import models
class distinct_status_supplier(serializers.Serializer):
        status_name= serializers.CharField()

class distinctbusinessUnit(serializers.Serializer):
        businessUnit= serializers.CharField()

class Reconciliation_status_wise_count(serializers.Serializer):
        from_date= serializers.CharField()
        to_date= serializers.CharField()
        status_name= serializers.ListField()
        supplier_name= serializers.ListField()
        bu= serializers.ListField()
        show_null_status = serializers.BooleanField(default=False)

class Reconciliation_completed_vendors(serializers.Serializer):
        supplier_name= serializers.ListField()
        bu= serializers.ListField()
        #status_name= serializers.ListField()
        from_date= serializers.CharField()
        to_date= serializers.CharField()
