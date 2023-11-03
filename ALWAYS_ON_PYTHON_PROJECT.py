import pymysql
import json
import paho.mqtt.client as paho
from paho import mqtt


# setting callbacks for different events to see if it works, print the message etc.
def on_connect(client, userdata, flags, rc, properties=None):
    print("CONNACK received with code %s." % rc)


# with this callback you can see if your publish was successful
def on_publish(client, userdata, mid, properties=None):
    print("mid: " + str(mid))


# print which topic was subscribed to
def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


# print message, useful for checking if it was successful
def on_message(client, userdata, msg):
    global mensaje
    print("Dato Reicibido de: "+msg.topic)
    topicdata = msg.topic.split('/')
    print(topicdata)
    try: mensaje = json.loads(msg.payload)
    except: mensaje = msg
    print(mensaje)
    try:
        try: IoT = topicdata[1]
        except: 
            print('no')
        try: viento = str(mensaje['VelViento'])
        except: 
            try: viento = str(mensaje['Viento'])
            except: viento = 'NaN'
        date = str(mensaje['Fecha'])
        try: luz = str(mensaje['IntSolar'])
        except: 
            try: luz = str(mensaje['Luz']) 
            except: 
                try: luz = str(mensaje['IntLuz'])
                except: luz = 'NaN'
        try: temp = str(mensaje['Temp'])
        except: 
            try: temp = str(mensaje['Temperatura'])
            except: temp = 'NaN'
        try: press = str(mensaje['PresAtm'])
        except: 
            try: press = str(mensaje['P_Atm'])
            except: press = 'NaN'
        try: humi = str(mensaje['Hum'])
        except: 
            try: humi = str(mensaje['Humedad'])
            except: humi = 'NaN'
        time = str(mensaje['Hora'])
        try: uv = str(mensaje['UV'])
        except: 
            try: uv = str(mensaje['UV']) 
            except: uv = 'NaN'
        datos = [IoT,time,date,viento,luz,temp,humi,press,uv]
        
        conn = pymysql.connect(
            host='localhost',
            user='userdb',
            password='pwddb!',
            db='proyecto_umes_2023',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        with conn.cursor() as cursor:
            for x in range(len(datos)):
                if datos[x] == '':
                    datos[x] = 'NaN'
        # Create a new record
            sql = "INSERT INTO `lecturas` (`IoT`, `Fecha`, `Hora`, `Temperatura`, `Humedad`, `Presion_Atmosferica`, `Intensidad_Luz`, `Viento`, `UV`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, (datos[0], datos[2], datos[1], datos[5], datos[6], datos[7], datos[4], datos[3], datos[8]))
        print("Record inserted successfully")
        conn.commit()
        conn.close()
        # Commit changes
    except Exception as e:
        print(e)
        pass
    

# using MQTT version 5 here, for 3.1.1: MQTTv311, 3.1: MQTTv31
# userdata is user defined data of any type, updated by user_data_set()
# client_id is the given name of the client
client = paho.Client(client_id="", userdata=None, protocol=paho.MQTTv5)
client.on_connect = on_connect

# enable TLS for secure connection
client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
# set username and password
client.username_pw_set("usermqtt", "pwdmqtt!")
# connect to HiveMQ Cloud on port 8883 (default for MQTT)
client.connect("36e9b7c4f0d8445489a9802addb355e3.s1.eu.hivemq.cloud", 8883)

# setting callbacks, use separate functions like above for better visibility
client.on_subscribe = on_subscribe
client.on_message = on_message
client.on_publish = on_publish

#mongodb+srv://Defii:gO9z9NWmRtAnbxV5@cluster0.ywbrjwd.mongodb.net/?retryWrites=true&w=majority
#client.subscribe("Reto_Umes/Estacion_2_Villa_Nueva_Zona_12/#", qos=2)
client.subscribe("Reto_Umes/#", qos=2)
client.subscribe("Reto_UMES/#", qos=2)
# loop_forever for simplicity, here you need to stop the loop manually
# you can also use loop_start and loop_stop 
client.loop_forever()
