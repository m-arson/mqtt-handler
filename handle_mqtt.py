import random
from paho.mqtt import client as mqtt_client
import threading
import requests
import json
import time
import mysql.connector

db = mysql.connector.connect(
	host="localhost",
	user="newuser",
	password="bitter",
	database="db_data"
)

c = db.cursor()

sql_syntax = "INSERT INTO tb_data (user_id, p1_v, p2_v, p3_v, p4_v, p5_v, p1_c, p2_c, p3_c, p4_c, p5_c, p1_pf, p2_pf, p3_pf, p4_pf, p5_pf, c1_pn, c2_pn, c3_pn, c1_bl, c2_bl, c3_bl, temp, humi, wind_speed) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"



p1_v = 0
p2_v = 0
p3_v = 0
p4_v = 0
p5_v = 0

p1_c = 0
p2_c = 0
p3_c = 0
p4_c = 0
p5_c = 0

p1_pf = 0
p2_pf = 0
p3_pf = 0
p4_pf = 0
p5_pf = 0

c1_pn = 0
c2_pn = 0
c3_pn = 0

c1_bl = 0
c2_bl = 0
c3_bl = 0

temp = 0
humi = 0
wind_speed = 0

api_key = "7a0f140b8501a25a5228fed32b1c34a5"
opw = "http://api.openweathermap.org/data/2.5/weather?lat=-5.125156&lon=119.463207&appid=7a0f140b8501a25a5228fed32b1c34a5"

user_id = "463bedcd352433fdc658866144b37144"

broker = 'broker.hivemq.com'
port = 1883
topic1 = "463bedcd352433fdc658866144b37144sm-proj-wxy-1230/pub/plug"
topic2 = "463bedcd352433fdc658866144b37144sm-proj-wxy-1230/pub/camera"

client_id = f'python-mqtt-{random.randint(0, 100)}'

val = (
	user_id,
	str(p1_v),
	str(p2_v),
	str(p3_v),
	str(p4_v),
	str(p5_v),
	str(p1_c),
	str(p2_c),
	str(p3_c),
	str(p4_c),
	str(p5_c),
	str(p1_pf),
	str(p2_pf),
	str(p3_pf),
	str(p4_pf),
	str(p5_pf),
	str(c1_pn),
	str(c2_pn),
	str(c3_pn),
	str(c1_bl),
	str(c2_bl),
	str(c3_bl),
	str(temp),
	str(humi),
	str(wind_speed)
)

def connect_mqtt() -> mqtt_client:
	def on_connect(client, userdata, flags, rc):
		if rc == 0:
			print("Connected to MQTT Broker!")
		else:
			print("Failed to connect, return code %d\n", rc)

	client = mqtt_client.Client(client_id)
	client.on_connect = on_connect
	client.connect(broker, port)
	return client


def subscribe(client: mqtt_client):
	def on_message(client, userdata, msg):
		global p1_v, p1_c, p1_pf;
		global p2_v, p2_c, p2_pf;
		global p3_v, p3_c, p3_pf;
		global p4_v, p4_c, p4_pf;
		global p5_v, p5_c, p5_pf;
		global c1_pn, c1_bl;
		global c2_pn, c2_bl;
		global c3_pn, c3_bl;
		tmp_msg = msg.payload.decode()
		tmp_msg_splitted = tmp_msg.split("/")
		
		if(tmp_msg_splitted[0] == "p01"):
			p1_v = tmp_msg_splitted[2]
			p1_c = tmp_msg_splitted[3]
			p1_pf = tmp_msg_splitted[4]
			
		elif(tmp_msg_splitted[0] == "p02"):
			p2_v = tmp_msg_splitted[2]
			p2_c = tmp_msg_splitted[3]
			p2_pf = tmp_msg_splitted[4]
			
		elif(tmp_msg_splitted[0] == "p03"):
			p3_v = tmp_msg_splitted[2]
			p3_c = tmp_msg_splitted[3]
			p3_pf = tmp_msg_splitted[4]
			
		elif(tmp_msg_splitted[0] == "p04"):
			p4_v = tmp_msg_splitted[2]
			p4_c = tmp_msg_splitted[3]
			p4_pf = tmp_msg_splitted[4]
			
		elif(tmp_msg_splitted[0] == "p05"):
			p5_v = tmp_msg_splitted[2]
			p5_c = tmp_msg_splitted[3]
			p5_pf = tmp_msg_splitted[4]
		elif(tmp_msg_splitted[0] == "c01"):
			c1_pn = tmp_msg_splitted[2]
			c1_bl = tmp_msg_splitted[1]
			
		elif(tmp_msg_splitted[0] == "c02"):
			c2_pn = tmp_msg_splitted[2]
			c2_bl = tmp_msg_splitted[1]
				
		elif(tmp_msg_splitted[0] == "c03"):
			c3_pn = tmp_msg_splitted[2]
			c3_bl = tmp_msg_splitted[1]

		#print("###########################")
		#print("P01 :")
		#print(p1_v, p1_c, p1_pf)
		#print("P02 :")
		#print(p2_v, p2_c, p2_pf)
		#print("P03 :")
		#print(p3_v, p3_c, p3_pf)
		#print("P04 :")
		#print(p4_v, p4_c, p4_pf)
		#print("P05 :")
		#print(p5_v, p5_c, p5_pf)
		#print("Camera 01")
		#print(c1_pn, c1_bl)
		#print("Camera 02")
		#print(c2_pn, c2_bl)
		#print("Camera 03")
		#print(c3_pn, c3_bl)
		#print("Temperature :", temp)
		#print("Humidity    :", humi)
		#print("Wind Speed  :", wind_speed)
		#print("###########################")

	client.subscribe(topic1)
	client.subscribe(topic2)
	client.on_message = on_message


def run():
	client = connect_mqtt()
	subscribe(client)
	client.loop_forever()

def open_weather_req():
	global temp, humi, wind_speed;
	while True:
		get_res = requests.get(opw)
		get_res_json = get_res.json()
		temp = round(get_res_json['main']['temp']-273.15, 2)
		humi = get_res_json['main']['humidity']
		wind_speed = get_res_json['wind']['speed']
		c.execute(sql, val)
		db.commit()
		time.sleep(1)

t1 = threading.Thread(target=open_weather_req, name="t1")

if __name__ == '__main__':
	t1.start()
	run()
