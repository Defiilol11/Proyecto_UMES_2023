from machine import Pin, SoftI2C, ADC, RTC
import time
import random
import ujson
import dht
import ntptime
import libys
from umqtt.simple import MQTTClient

mantenimiento = False
i2c = SoftI2C(scl=Pin(12), sda=Pin(11), freq=100000)
pir = Pin(05, Pin.IN, Pin.PULL_UP)
last_interrupt_time = 0
por = Pin(08, Pin.IN, Pin.PULL_UP)

#funciones
def mante(pin):
    global mantenimiento
    global last_interrupt_time
    current_time = time.ticks_ms()
    if time.ticks_diff(current_time, last_interrupt_time) > 1000:
        mantenimiento = not mantenimiento
        if mantenimiento == True:
            libys.char(' mantenimiento  ')
            libys.char('                ')
            while True:
                if pir.value() == 0:
                    libys.char('regresando a la ')
                    libys.char('   normalidad   ')
                    time.sleep(1)
                    break
        last_interrupt_time = current_time

def connect(id, pswd):
    ssid = id
    password = pswd
    if station.isconnected() == True:
        pass
    return

def connectMQTT():
  client = MQTTClient(client_id=b"TeneT-IoT",
  server=b"36e9b7c4f0d8445489a9802addb355e3.s1.eu.hivemq.cloud",
  port=8883,
  user=b"tenet",
  password=b"Testenet1!",
  keepalive=7200,
  ssl=True,
  ssl_params={'server_hostname':'36e9b7c4f0d8445489a9802addb355e3.s1.eu.hivemq.cloud'}
  )

  client.connect()
  return client

def publish(topic, value):
    print('message sended')
    client.publish(topic, str(value))

#definiciones de funciones
pir.irq(trigger=Pin.IRQ_FALLING, handler=mante)
light = ADC(Pin(14))
sensor = dht.DHT11(Pin(13))
ntptime.settime()
(year, month, day, weekday, hour, minute, second, milisecond) = RTC().datetime()
RTC().init((year, month, day, weekday, hour-6, minute, second, milisecond))
client = connectMQTT()

try:
    while True:
        client = connectMQTT()
        sensor.measure() 
        t = sensor.temperature()
        h = sensor.humidity()
        pres = BMP280(bus).read()
        sensval = light.read()
        light = sensval * (0.5)
        val = ADC(Pin(20))
        wind = ((val/100))
        date = "{:02d}/{:02d}/{}".format(RTC().datetime()[2], RTC().datetime()[1], RTC().datetime()[0])  
        times = "{:02d}:{:02d}:{:02d}".format(RTC().datetime()[4], RTC().datetime()[5], RTC().datetime()[6])
        UVindex = libys.light(light)
        data ={"Fecha":date,"Hora":times,"Temperatura":t,"Humedad":h,"Luz":UVindex,"Viento":wind,"P_Atm":pres}
        mqtt_send = ujson.dumps(data)
        publish('Reto_Umes/Estacion_2_Villa_Nueva_Zona_12/sensores.json',mqtt_send)
        s = 30
        x = 0
        while x < s:
            libys.char("Luz:     "+str(UVindex)+" lm")
            libys.char("Presion:"+str(pres)+" hPa")
            time.sleep(5)
            libys.char("Temperatura:"+str(t)+" C")
            libys.char("Humedad:    "+str(h)+" %")
            time.sleep(5)
            x = x + 1
except Exception as e:
    client = connectMQTT()
    print(e)
    libys.char('hubo un error   ')
    libys.char('revisa el codigo')