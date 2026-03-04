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
from collections import deque
import queue as pyqueue

# --- SIGMA UHF FIFO MATCHING (RFID <-> PESO) ---
# Colas inter-proceso. RFID (thread) -> tag_queue; Peso (process) -> peso_queue
tag_queue = mp.Queue()  # (ts, epc_hex)
peso_queue = mp.Queue() # (ts, peso, estado, fecha, viaje_id, cantidad, pesovastago)

#####################################################################
######               COMANDOS RFID                            #######
#####################################################################

CMD_POTENCIA = bytes.fromhex("BB 00 B6 00 02 0A 28 EA 7E") # 26dBm
CMD_REGION   = bytes.fromhex("BB 00 07 00 01 02 0A 7E")    # US Region
CMD_FREQ     = bytes.fromhex("BB 00 AB 00 01 1A C6 7E")    # Canal RF
CMD_LEER     = bytes.fromhex("BB 00 27 00 03 22 FF FF 4A 7E") # Lectura Continua
CMD_STOP     = bytes.fromhex("BB 00 28 00 00 28 7E")




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
    def __init__(self, peso_queue):
        self.peso_queue = peso_queue

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
                self.peso_queue.put((time.time(), peso, self.estado, fecha, self.viaje_id, self.cantidad, self.pesovastago))

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
    """Lee tags UHF en continuo y los pone en tag_queue (FIFO).

    Qué hace:
    - Configura el/los lectores (REGION, FREQ, POTENCIA, LEER).
    - Lee tramas en modo inventario continuo.
    - Extrae EPC en HEX.
    - Aplica anti-duplicado por EPC (tiempo_relectura segundos).
    - Encola (timestamp, EPC) en tag_queue.
    """

    def __init__(self, tag_queue):
        super().__init__()
        self.daemon = True
        self.tag_queue = tag_queue

        self.ultimas_vistas = {}   # epc -> ts
        self.tiempo_relectura = 3.0

        self.lectores = []

    def _puertos_candidatos(self):
        puertos = []
        for p in serial.tools.list_ports.comports():
            dev = getattr(p, "device", "")
            if dev and (("ttyUSB" in dev) or ("ttyACM" in dev)):
                puertos.append(dev)
        return puertos

    def _configurar_lector(self, ser):
        ser.write(CMD_REGION);   time.sleep(0.08)
        ser.write(CMD_FREQ);     time.sleep(0.08)
        ser.write(CMD_POTENCIA); time.sleep(0.08)
        ser.write(CMD_LEER);     time.sleep(0.05)

    def _detectar_lectores(self):
        self.lectores.clear()
        puertos = self._puertos_candidatos()
        if not puertos:
            print("RFID: no se detectaron lectores (ttyUSB/ttyACM).")
            return False

        for puerto in puertos:
            try:
                ser = serial.Serial(puerto, 115200, timeout=0.05)
                self._configurar_lector(ser)
                self.lectores.append(ser)
                print("RFID conectado en", puerto)
            except Exception as e:
                print("RFID: error conectando", puerto, repr(e))

        return len(self.lectores) > 0

    def _publicar_tag(self, epc_hex):
        try:
            self.tag_queue.put((time.time(), epc_hex))
            print("RFID TAG:", epc_hex)
        except Exception as e:
            print("RFID: no se pudo encolar TAG", repr(e))

    def _procesar_trama(self, trama):
        # Frame inicia en 0xBB y el payload length va en [3:5]
        if len(trama) < 8 or trama[0] != 0xBB:
            return

        pl = (trama[3] << 8) | trama[4]
        params = trama[5:5+pl]
        if len(params) < 4:
            return

        # EPC variable (HEX)
        epc_hex = params[3:].hex().upper()
        if not epc_hex:
            return

        ahora = time.time()
        last = self.ultimas_vistas.get(epc_hex)

        # Anti duplicado (equivalente a ordenar.py en vivo)
        if (last is None) or ((ahora - last) > self.tiempo_relectura):
            self.ultimas_vistas[epc_hex] = ahora
            self._publicar_tag(epc_hex)

    def run(self):
        # Detectar lectores y reintentar si no hay
        if not self._detectar_lectores():
            while True:
                time.sleep(2.0)
                if self._detectar_lectores():
                    break

        buffers = {id(ser): bytearray() for ser in self.lectores}

        while True:
            # Si se quedaron sin lectores, re-detectar
            if not self.lectores:
                time.sleep(2.0)
                self._detectar_lectores()
                buffers = {id(ser): bytearray() for ser in self.lectores}
                continue

            for ser in list(self.lectores):
                try:
                    buf = buffers.setdefault(id(ser), bytearray())
                    chunk = ser.read(ser.in_waiting or 1)
                    if chunk:
                        buf.extend(chunk)

                    # Parseo: BB .... PL .... 7E  (longitud total = 7 + PL)
                    while True:
                        ini = buf.find(b'\xBB')
                        if ini < 0:
                            buf.clear()
                            break
                        if ini > 0:
                            del buf[:ini]
                        if len(buf) < 5:
                            break

                        pl = (buf[3] << 8) | buf[4]
                        frame_len = 7 + pl
                        if len(buf) < frame_len:
                            break

                        trama = bytes(buf[:frame_len])
                        del buf[:frame_len]

                        # Respuesta inventario
                        if len(trama) >= 3 and trama[1] == 0x02 and trama[2] == 0x22:
                            self._procesar_trama(trama)

                except Exception as e:
                    print("RFID: lector desconectado/error:", repr(e))
                    try:
                        self.lectores.remove(ser)
                    except Exception:
                        pass
                    try:
                        ser.close()
                    except Exception:
                        pass

            time.sleep(0.01)


class Matcher(threading.Thread):
    """Empareja FIFO tags y pesos sin bloquear el pesaje.

    Regla:
    - Si hay PESO y TAG disponibles: PESO1->TAG1, PESO2->TAG2...
    - Si llega un peso y no hay tag por 'ventana' segundos: se guarda con 'NO_TAG' para NO frenar la operación.
    - Si SIGMA abre un viaje nuevo, se limpian colas para evitar mezcla entre viajes.
    """

    def __init__(self, tag_queue, peso_queue, ventana_seg=2.0):
        super().__init__()
        self.daemon = True
        self.tag_queue = tag_queue
        self.peso_queue = peso_queue
        self.ventana = float(ventana_seg)

        self.tags = deque()   # (ts, epc)
        self.pesos = deque()  # (ts, peso, estado, fecha, viaje_id, cantidad, pesovastago)
        self.viaje_actual = None

    def _drain(self):
        # Pasar lo que haya en las mp.Queue a deques locales (más rápido)
        while True:
            try:
                self.tags.append(self.tag_queue.get_nowait())
            except pyqueue.Empty:
                break
            except Exception:
                break

        while True:
            try:
                self.pesos.append(self.peso_queue.get_nowait())
            except pyqueue.Empty:
                break
            except Exception:
                break

    def run(self):
        while True:
            try:
                viaje = funciones.Get_last_viaje()
                if self.viaje_actual is None:
                    self.viaje_actual = viaje

                # Detectar cambio de viaje para limpiar colas
                if viaje != self.viaje_actual:
                    self.tags.clear()
                    self.pesos.clear()
                    self._drain()  # drenar colas de mp.Queue también
                    self.tags.clear()
                    self.pesos.clear()
                    self.viaje_actual = viaje
                    print("MATCHER: CAMBIO DE VIAJE -> COLAS LIMPIAS")

                self._drain()

                # Emparejar FIFO mientras haya de ambos
                while self.pesos and self.tags:
                    ts_p, peso, estado, fecha, viaje_id, cantidad, pesovastago = self.pesos.popleft()
                    ts_t, epc = self.tags.popleft()

                    funciones.Update_db(peso, estado, fecha, viaje_id, cantidad, epc, pesovastago)
                    print("MATCHER: EMPAREJADO", peso, epc)

                # Si hay pesos sin tag, esperar ventana; luego guardar NO_TAG
                if self.pesos and (not self.tags):
                    ts_p, peso, estado, fecha, viaje_id, cantidad, pesovastago = self.pesos[0]
                    if (time.time() - ts_p) > self.ventana:
                        self.pesos.popleft()
                        funciones.Update_db(peso, estado, fecha, viaje_id, cantidad, "NO_TAG", pesovastago)
                        print("MATCHER: PESO SIN TAG (NO_TAG)", peso)

            except Exception as e:
                print("MATCHER: ERROR", repr(e))

            time.sleep(0.02)


matcher = Matcher(tag_queue, peso_queue, ventana_seg=2.0)
matcher.start()
