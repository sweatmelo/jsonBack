import requests


f = open('C:/Users/kappe/Desktop/TestUpload.txt', 'rb')

files = {"file": ("Nameeee.txt", f)}

base_url = "http://127.0.0.1:5000/"
resp = requests.post(base_url + "/upload", files=files)
print(resp.json())