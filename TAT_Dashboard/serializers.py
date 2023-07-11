from rest_framework import serializers
#from .import models
class CountSerializer(serializers.Serializer):
        dock_name = serializers.CharField()
        store_name = serializers.CharField()
        from_date = serializers.CharField()
        to_date = serializers.CharField()