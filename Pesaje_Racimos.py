# -*- coding: utf-8 -*-
#####################     SIOMA S.A.S    ############################
#           Codigo para captacion de racimos Sigma                  #
#####################################################################
__author__ = 'cristianrojas'
######               Importacion de librerias                 #######
import time
import threading
import multiprocessing as mp

import smbus
import serial
import credentials
import numpy as np
import bat
import funciones
import hxsigma as hs
import backup
import datetime
import x708
import rfiduhf



#####################################################################
######               Declaracion de variables                 #######
online = 0
intentosConexion = 0
TIEMPOENVIO = 300
HXDEBUG = False
SYNCTABLAS = ["tipo_cintas", "esquema_cintas", "estomas", "lotes", "motivo_rechazos", "personas", "fincas", "staff", "wifis", "defectos"]
HORACORREO = 18  # Formato de 24 horas
CANTDATOSRACIMO = 35  # Cantidad de datos encontrados por analisis *60% que son los datos usados del vector
PERIODOVALIDACION = 1  # Cantidad de dias para exigir validacion
MAXLIMITVECPESOS = 500 # Cantidad maxima de datos a usar para el vector de pesos
TIEMPO_CHECK_BATERIA = 30 # segundos
VOLTAJE_BATERIA_APAGADO = 3.1
#####################################################################
######               Declaracion de pines                     #######
R = 17
G = 18
B = 27
GND = 22
DOUT = 10
SCK = 9


#####################################################################
######               Hilos de ejecucion                       #######
#### Hilo encargado de leer los valores de peso
class GetPeso(mp.Process):
    def __init__(self):
        mp.Process.__init__(self)
        # Inicializacion de las variables para lectura de pesos
        self.lectura = 0
        self.lecturaAnterior = 0
        self.estadoActual = False
        self.estadoAnterior = False
        self.vecPesos = []
        self.i = 0
        self.MAX_PESO = 80  # Define el peso maximo que recibira el sistema
        self.UPPER_RANGE = 6  # Por defecto debe estar en 6, valor desde el cual se detecta un racimo
        self.DOWN_RANGE = 6  # Por defecto debe estar en 6, valor desde el cual se guarda un racimo
        self.RETRASOS = 8  # Por defecto debe estar en 8, retrasos en el filtro de los datos
        self.MEAN_DATA = 3  # Por defecto debe estar en 3
        self.zeroInit = 0
        self.zeroIniprom = 0
        self.vectorFiltro = []
        self.zeroInicial = []
        self.lenVecZero = 500
        for i in range(0,self.lenVecZero):
            self.zeroInicial.append(0)
        self.vectorZero = []
        self.ErrorCero = False
        self.tend = 0
        self.tin = 0
        self.t_errortotal = 0
        self.t_save_cero = 900  #Segundos
        self.LEN_DATOS_PESO = 0.6
        self.DELTA_PESO = 7 # Kg si se presenta una variacion en peso mayor a este delta se generara un cambio de estado
        self.RFIDserial = ''
        self.RFID_ON = 0
        self.vastago = float(funciones.Get_parametro("Vastago"))
        self.pesovastago = 0.0

    def Llenar_vector(self):
        for i in range(0, self.RETRASOS):
            lectura = sensor.Get_lectura(self.MEAN_DATA)
            if not lectura:
                print("ERROR FATAL DE LECTURA")
                while True:
                    pass
            self.vectorFiltro.append(lectura)

    def Get_lectura(self):
        self.lecturaAnterior = self.lectura
        dato = sensor.Get_lectura(self.MEAN_DATA)
        self.vectorFiltro = funciones.Shift(self.vectorFiltro, dato)
        funciones.Log_datos(self.vectorFiltro[len(self.vectorFiltro)-1], "/home/pi/datos/logData/SinFiltro.txt")
        self.vectorFiltro = funciones.Filtro_picos(self.vectorFiltro, pd=0.8)
        self.lectura = self.vectorFiltro[0]
        funciones.Log_datos(self.vectorFiltro[0], "/home/pi/datos/logData/ConFiltro.txt")

    def Actualizar_estado(self):
        self.estadoAnterior = self.estadoActual
        # Se valida si el peso pasa por el limite de subida o si en las ultimas 4 lecturas hubo un cambio mayor al delta peso
        if self.UPPER_RANGE < self.lectura < self.MAX_PESO or (self.vectorFiltro[0]-self.vectorFiltro[3]) > self.DELTA_PESO:
            self.estadoActual = True
            RFID_ON = int(funciones.Get_parametro("RFID_ON"))
            if RFID_ON == 1:
                funciones.Set_parametro("RFID_ON", 0)
                self.RFIDserial = funciones.Get_parametro("RFID_SERIAL")
        # Se valida si el peso pasa por el limite de bajada o si en las ultimas 4 lecturas hubo un cambio menor al delta peso
        elif self.lectura < self.DOWN_RANGE or (self.vectorFiltro[0]-self.vectorFiltro[3]) < (-1*self.DELTA_PESO):
            self.estadoActual = False

    def Guardar_datos(self):
        self.vecPesos.append(self.lectura)

    def Update_db(self):
        self.vecPesos = self.vecPesos[0:MAXLIMITVECPESOS]
        vecPesosFiltrados = self.vecPesos
        self.vecPesos.sort(reverse=True)
        self.cantidad = int(round(len(self.vecPesos) * self.LEN_DATOS_PESO))
        #print("Cantidad de datos utilizados en la lectura: ", self.cantidad)
        if self.cantidad > 10:
            delete = self.vecPesos[self.cantidad:len(self.vecPesos)]
            for i in range(0, len(delete)):
                vecPesosFiltrados.remove(delete[i])
            self.vecPesos = []
            peso = round(np.average(vecPesosFiltrados), 2)

            #################### Validaciones ####################
            self.estado = 0
            peso = peso - TARA

            if peso < PESOMINIMO:
                self.estado = 2  # Bajo peso
            elif peso > PESOMAXIMO:
                self.estado = 3  # Alto Peso

            lastFecha = funciones.Get_last_racimito()
            fecha = datetime.datetime.strptime(funciones.Actualizar_hora(), '%Y-%m-%d %H:%M:%S')

            if lastFecha is None:
                deltaSegundos = 10000
            elif lastFecha > fecha:
                print("Se han movido viajes, no se registraran datos")
                return None
            else:
                deltaSegundos = (fecha-lastFecha).seconds

            if (deltaSegundos > TIEMPOMINIMO or funciones.Get_last_viaje() is None or int(funciones.Get_parametro("nuevo_viaje")) == 1) and self.estado == 0:
                peso = peso - TARAPRIMERO
                if peso < PESOMINIMO:
                    self.estado = 2  # Bajo peso
                elif peso > PESOMAXIMO:
                    self.estado = 3  # Alto Peso
                else:
                    funciones.Set_fecha_final()
                    funciones.Crear_viaje()
                    funciones.Set_parametro("nuevo_viaje", 0)

            self.viaje_id = funciones.Get_last_viaje()
            if self.estado == 0:
                self.pesovastago = round((float(peso) * float(self.vastago)), 2)
                funciones.Update_db(peso, self.estado, fecha, self.viaje_id, self.cantidad, self.RFIDserial,self.pesovastago)

        else:
            print("Cantidad de datos insuficientes para obtener un peso valido")


    def Validar_cero(self, zeroActual):
        try:
            n = len(zeroActual)
            if n > 0:
                nporcent = int(n * 0.1)
                ValorZeroActual = np.mean(zeroActual[nporcent:(n-nporcent)])
                ValorZeroActual = round(ValorZeroActual, 4)
                #error = abs(self.zeroIniprom - ValorZeroActual)
                error = ValorZeroActual
                #print("Error: ", error)
                if error > 0.055:
                    #print("FlagErrorCero")
                    self.ErrorCero = True
                    self.t_errortotal = self.t_errortotal + (self.tend - self.tin)
                    #print("Horas de error total: ",(self.t_errortotal/3600))
                    #Se guardan el tiempo cuando se ha acumulado media hora
                    if self.t_errortotal >= self.t_save_cero:
                        funciones.Set_parametro("Horas_dia_1",(self.t_errortotal/3600))

        except Exception as e:
            print("ERROR VALIDANDO CERO: ", repr(e))

    def run(self):
        self.Llenar_vector()
        czero = 0
        badzero = 0
        while True:
            try:
                self.Get_lectura()
                if self.zeroInit == 0:
                    czero+=1
                    if (-2.0 < self.lectura < 2.0):
                        self.zeroInicial = funciones.Shift(self.zeroInicial, abs(self.lectura))
                        if czero >= 550:
                            self.zeroIniprom = abs(np.mean(self.zeroInicial[24:474]))
                            if self.zeroIniprom > 0.05:
                                print("Error en el cero")
                            print("Cero Inicial: ",self.zeroIniprom)
                            self.zeroInit = 1
                            self.tin = time.time()
                    else:
                        badzero+=1
                        if badzero >= 150:
                            print("Cero inestable")
                            self.zeroIniprom = abs(np.mean(self.zeroInicial))
                            self.zeroInit = 1
                            self.tin = time.time()
                    self.vectorZero = self.zeroInicial

                if (self.lectura < 0.3) and self.zeroInit == 1:
                    self.vectorZero = funciones.Shift(self.vectorZero, abs(self.lectura))

                self.tend = time.time()
                if (self.tend - self.tin) > 15:
                    self.Validar_cero(self.vectorZero)
                    self.tin = time.time()

                self.Actualizar_estado()

                if self.estadoActual:
                    self.Guardar_datos()
                elif not self.estadoActual and self.estadoAnterior:
                    self.Update_db()
                    self.RFIDserial = ''
                    self.VectorZero = self.zeroInicial
            except Exception as E:
                print("ERROR DE LECTURA DE PESO: ", repr(E))


### Hilo de subida de datos a la web
class UpLoad(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.correoEnviado = False

    def run(self):
        while True:
            try:
                # Ejecutar la funcion de subida de datos a la web
                funciones.Sync()
                hora = datetime.datetime.strptime(funciones.Actualizar_hora(), '%Y-%m-%d %H:%M:%S')
                if hora.time().hour >= HORACORREO and funciones.Viajes_sin_revisar() > 0 and not self.correoEnviado:
                    subject = "Viajes sin revisar"
                    body = "El equipo " + estomaId + " tiene " + str(funciones.Viajes_sin_revisar()) + \
                           " viajes sin revisar"
                    funciones.Web_noti_finca(subject=subject, datos=body)
                    self.correoEnviado = True
            except Exception as E:
                print("ERROR SINCRONIZANDO: " + repr(E))
            time.sleep(TIEMPOENVIO)


# Hilo de validacion de bateria
class CheckBat(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        try:
            self.bus = smbus.SMBus(1)  # 0 = /dev/i2c-0 (port I2C0), 1 = /dev/i2c-1 (port I2C1)
        except Exception as e:
            print("NO SE ENCUENTRA PLACA PARA LECTURA DE BATERIA" + repr(e))

    def run(self):
        global TIEMPO_CHECK_BATERIA
        while True:
            try:
                Voltaje = round(x708.readVoltage(self.bus), 2)
                #print("Voltaje: ",float(Voltaje))
                funciones.Set_parametro("Voltaje", Voltaje)
                bateria = int(round(x708.readCapacity(self.bus), 2))
                #print("% Bateria:", bateria)
                #print("Capacity: ", bateria * 256)
                funciones.Set_parametro("bateria", bateria)
                estado_cargador = x708.estado_cargador
                if float(Voltaje) < VOLTAJE_BATERIA_APAGADO and not estado_cargador:
                    print("Bateria baja, el equipo se apagara")
                    funciones.Cmd_line("sudo x708softsd.sh")

            except Exception as e:
                print("ERROR VALIDANDO BATERIA: " + repr(e))

            time.sleep(TIEMPO_CHECK_BATERIA)


class RFIDRead(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

        try:
            self.ser = serial.Serial("/dev/ttyS0",
                                     baudrate = 9600,
                                     parity=serial.PARITY_NONE,
                                     stopbits=serial.STOPBITS_ONE,
                                     bytesize=serial.EIGHTBITS,
                                     timeout=1)
        except Exception as e:
            pass
            #print("ERROR EN PUERTO SERIAL RFID: ", repr(e))

        self.RFIDdata = ''
        self.RFIDflag = 0

    def run(self):
        while True:
            try:
                self.RFIDdata = self.ser.readline().strip().decode("utf-8").strip()
                if len(self.RFIDdata) > 10:
                    self.RFIDflag = 1
                    self.RFIDdata = self.RFIDdata[len(self.RFIDdata)-8:]
                    self.RFIDdata = str(int(self.RFIDdata, base=16))
                    funciones.Set_parametro('RFID_ON', self.RFIDflag)
                    funciones.Set_parametro('RFID_SERIAL',self.RFIDdata)
                    #print("RFID Tag detected:", self.RFIDdata)

            except Exception as e:
                #print("ERROR EN LECTURA RFID: " + repr(e))
                pass
#####################################################################
######               Metodos de inicializacion                #######


### Inicializacion de base de datos
funciones.Conect_db_parametros()

### Inicializacion de parametros
funciones.Set_parametro("cant_rapido", CANTDATOSRACIMO)
funciones.Set_parametro("lb", 0)
funciones.Set_parametro("RFID_ON",0)
funciones.Set_parametro("nuevo_viaje", 0)

### Validacion de la conexion a internet
while (online == 0) & (intentosConexion < 5):
    try:
        online = funciones.Test_online()
        if online == 0:
            intentosConexion += 1
        print(intentosConexion)
    except:
        intentosConexion = intentosConexion + 1
        print(intentosConexion)

# Guardar el estado del contenido de las tablas
contentTablas = funciones.Check_contenido_tablas(SYNCTABLAS)

### Actualizacion de tablas
if online or not contentTablas:
    ### Aviso de inicio al servidor ##### QUITAR
    estomaInfo = credentials.Get_Estoma_Info()
    estomaId = estomaInfo[0]
    print(funciones.Web_conex("inicio", estomaId, timeout=5))
    # Vectores para el manejo de los estados de descarga de las tablas
    estadoTablas = []
    estados = []
    # Concatenar el estado cero al vector de tablas
    for tabla in SYNCTABLAS:
        estadoTablas.append([tabla, 0])
    # ejecutar el ciclo mientras alguno de los estados de las tablas siga siendo cero
    [estados.append(estado[1]) for estado in estadoTablas]
    while min(estados) < 1:
        try:
            for tabla in estadoTablas:
                nombre = tabla[0]
                estado = tabla[1]
                if estado == 0:
                    funciones.Actualizar_tabla(nombre)
                if funciones.Check_contenido_tablas([tabla[0]]):
                    tabla[1] = 1

            print(estadoTablas)

            estados = []
            [estados.append(estado[1]) for estado in estadoTablas]

        except Exception as e:
            print("ERROR ACTUALIZANDO TABLAS: ", repr(e))

cod_barcadillero = funciones.Get_barcadillero()
funciones.Set_parametro("barcadillero_codigo", cod_barcadillero)

vastago = funciones.Get_vastago()
funciones.Set_parametro("Vastago", vastago)

PESOMINIMO = float(funciones.Get_parametro_estoma("peso_minimo_racimitos"))
print("Peso minimo: ", PESOMINIMO)
PESOMAXIMO = float(funciones.Get_parametro_estoma("peso_maximo_racimitos"))
print("Peso maximo: ", PESOMAXIMO)
TARA = float(funciones.Get_parametro_estoma("tara_racimitos"))
print("Tara: ", TARA)
TARAPRIMERO = float(funciones.Get_parametro_estoma("tara_primer_racimito"))
print("Tara primer racimo: ", TARAPRIMERO)
TIEMPOMINIMO = float(funciones.Get_parametro_estoma("tiempo_minimo_racimitos"))
print("Tiempo minimo entre racimos: ", TIEMPOMINIMO)
funciones.Save_wifi()


### Crear parametro de lote_default
cursor, conectar = funciones.Create_cursor(json=True)
conectar.commit()
cursor.execute("select * from lotes limit 1")
recs = cursor.fetchall()
rows = [dict(rec) for rec in recs]
funciones.Set_parametro("lote_default", rows[0]['lote_id'])


#### Validacion de validacion diaria
lastValidacion = funciones.Get_Last_Validacion()
fechaHoy = funciones.Actualizar_hora(dia=1)
print("Ultima Validacion: ", lastValidacion, "Fecha actual: ", fechaHoy)
if lastValidacion is not None:
    print("Diferencia: ", (fechaHoy - lastValidacion.date()).days)


if lastValidacion is None or (fechaHoy - lastValidacion.date()).days >= PERIODOVALIDACION:
    calibracion = 2
    validacion = 0
else:
    print("Validacion de equipo se encuntra al dia")
    validacion = 0


#Revision cero
funciones.Get_info_cero()

### Inicializacion de sensor de peso
sensor = hs.HxSigma(debug=HXDEBUG)
funciones.Set_parametro("validacion", validacion)

funciones.Set_parametro('estado_hx', 6)
print("INICIANDO")

### Inicializacion de hilos
lecturaPeso = GetPeso()
sincronizacion = UpLoad()
bateria = CheckBat()
# RFID = RFIDRead()  # replaced by RFIDUHF

lecturaPeso.start()
sincronizacion.start()
bateria.start()
# RFID.start()  # replaced
rfid = rfiduhf.RFIDUHF()
thread_rfid = threading.Thread(target=rfid.run)
thread_rfid.start()


funciones.Set_parametro('estado_hx', -666)
print("BASCULA LISTA PARA PESAR")