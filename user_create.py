from django.contrib.auth.models import User
import pandas as pd
df = pd.read_excel("cv_django_users.xlsx", keep_default_na = False)
for index, each in df.iterrows():
    # vendorcode = each[0]
    # print(type(vendorcode))
    print(each[0])
    a1 = User.objects.filter(username = each[0])
    print(a1)
    if len(a1) == 0:
        # Create User
        pass
        #user = User(username = each[4],first_name = each[5], last_name =each[6],email=each[7],is_staff=each[8],is_active=each[9])
        user = User(username = each[4],first_name = each[2], last_name =each[3],email=each[4],is_staff=each[6],is_active=each[7])
        user.is_staff = True
        user.set_password(each[5])
        user.save()