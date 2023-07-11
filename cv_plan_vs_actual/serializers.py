from rest_framework import serializers
#from .import models
class distinctSerializer(serializers.Serializer):
        plant_id = serializers.CharField()

class distinctstatusSerializer(serializers.Serializer):
        #Status = serializers.CharField()
        plant_id = serializers.CharField()
        shop_type = serializers.ListField()

class distinctshiftnameSerializer(serializers.Serializer):
        shiftname = serializers.CharField()

class distinctshoptypeSerializer(serializers.Serializer):
        plant_id = serializers.CharField()
        shop_type = serializers.ListField()

class distinctLineSerializer(serializers.Serializer):
        plant_id = serializers.CharField()
        shop_type = serializers.ListField()
        shop = serializers.ListField()

class tableviewSerializer(serializers.Serializer):
        plant_id = serializers.CharField()
        shop_type = serializers.ListField()
        shop = serializers.ListField()
        Status = serializers.ListField()
        shift = serializers.ListField()
        line = serializers.ListField()
        from_date = serializers.CharField()
        to_date = serializers.CharField()

class hr_month_yr_Serializer(serializers.Serializer):
        plant_id = serializers.CharField()
        shop_type = serializers.ListField()
        shop = serializers.ListField()
        Status = serializers.ListField()
        shift = serializers.ListField()
        line = serializers.ListField()
        duration=serializers.CharField()
        from_date = serializers.CharField()
        to_date = serializers.CharField()

class CustometableviewSerializer(serializers.Serializer):
        plant_id = serializers.CharField()
        shop_type = serializers.ListField()
        shop = serializers.ListField()
        Status = serializers.ListField()
        shift = serializers.ListField()
        line = serializers.ListField()
        from_date = serializers.CharField()
        to_date = serializers.CharField()
        searchtext=serializers.CharField()
        start=serializers.IntegerField()
        colorder=serializers.CharField()
        coltype=serializers.CharField()
        size=serializers.IntegerField()
        shop_type_small = serializers.ListField()