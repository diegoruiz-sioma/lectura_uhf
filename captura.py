# -*- coding: utf-8 -*-
import serial
import serial.tools.list_ports
import time
import threading
import os
import csv
from datetime import datetime

# --- CONFIGURACION DE HARDWARE ---
CMD_POTENCIA = bytes.fromhex("BB 00 B6 00 02 0A 28 EA 7E")
CMD_REGION   = bytes.fromhex("BB 00 07 00 01 02 0A 7E")
CMD_FREQ     = bytes.fromhex("BB 00 AB 00 01 1A C6 7E")
CMD_LEER     = bytes.fromhex("BB 00 27 00 03 22 FF FF 4A 7E")
CMD_STOP     = bytes.fromhex("BB 00 28 00 00 28 7E")

class VectorSensorTag:
    def __init__(self, epc, puerto):
        self.epc = epc
        self.puerto = puerto
        self.ts_primera = time.time()
        self.ultimo_avistamiento = self.ts_primera
        self.lista_rssi = []
        self.contador_lecturas = 0

    def registrar(self, rssi):
        self.ultimo_avistamiento = time.time()
        self.lista_rssi.append(rssi)
        self.contador_lecturas += 1

class GestorSigmaIndependiente:
    def __init__(self):
        self.archivo_log = "reporte_por_sensor.csv"
        self.memoria_viva = {} 
        self.bloqueo_final = set()
        self.lock = threading.Lock()
        self.running = False
        self.serials = []
        
        # --- CONFIGURACION DE TIEMPOS: CAMBIADO A 20 SEGUNDOS ---
        self.timeout_salida = 20.0  
        # -------------------------------------------------------

        if not os.path.exists(self.archivo_log):
            self._preparar_csv()

    def _preparar_csv(self):
        with open(self.archivo_log, "a", newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                "Fecha", "HORA_PRIMERA_LECTURA", "Hora_Ultima_Lectura", 
                "Tag_ID", "Sensor_Puerto", "Lecturas_Totales", "RSSI_Max", "RSSI_Promedio"
            ])

    def start(self):
        puertos = [p.device for p in serial.tools.list_ports.comports() if 'ttyUSB' in p.device]
        if not puertos:
            print("No hay sensores USB detectados.")
            return False
        
        self.running = True
        threading.Thread(target=self._monitor_finalizacion, daemon=True).start()
        
        for p in puertos:
            threading.Thread(target=self._capturadora_hardware, args=(p,), daemon=True).start()
        
        print("SISTEMA ACTIVO - Espera de 20 segundos configurada.")
        return True

    def _capturadora_hardware(self, puerto):
        try:
            ser = serial.Serial(puerto, 115200, timeout=0.01)
            self.serials.append(ser)
            ser.write(CMD_REGION); time.sleep(0.1)
            ser.write(CMD_FREQ); time.sleep(0.1)
            ser.write(CMD_POTENCIA); time.sleep(0.1)
            ser.write(CMD_LEER)

            buffer_crudo = bytearray()
            while self.running:
                if ser.in_waiting:
                    buffer_crudo.extend(ser.read(ser.in_waiting))
                    while True:
                        inicio = buffer_crudo.find(b'\xBB')
                        if inicio < 0: buffer_crudo.clear(); break
                        if inicio > 0: del buffer_crudo[:inicio]
                        fin = buffer_crudo.find(b'\x7E', 1)
                        if fin < 0: break
                        trama = bytes(buffer_crudo[:fin + 1])
                        del buffer_crudo[:fin + 1]
                        self._procesar_individual(trama, puerto)
                else:
                    time.sleep(0.001)
        except Exception as e:
            print("Error en " + str(puerto) + ": " + str(e))

    def _procesar_individual(self, trama, puerto):
        if len(trama) >= 19 and trama[1] == 0x02:
            payload = trama[2:-1] if trama[-1] == 0x7E else trama[2:]
            tag_id = payload[6:18].hex().upper()
            rssi = payload[-2]
            
            clave_unica = tag_id + "_" + puerto
            
            with self.lock:
                if clave_unica in self.bloqueo_final:
                    return

                if clave_unica not in self.memoria_viva:
                    self.memoria_viva[clave_unica] = VectorSensorTag(tag_id, puerto)
                    print("CAPTURADO: " + tag_id + " en " + puerto)
                
                self.memoria_viva[clave_unica].registrar(rssi)

    def _monitor_finalizacion(self):
        while self.running:
            time.sleep(1.0)
            ahora = time.time()
            vectores_a_cerrar = []
            with self.lock:
                for clave, obj in list(self.memoria_viva.items()):
                    if (ahora - obj.ultimo_avistamiento) > self.timeout_salida:
                        vectores_a_cerrar.append(obj)
                        del self.memoria_viva[clave]
                        self.bloqueo_final.add(clave)

            for obj in vectores_a_cerrar:
                self._escribir_final(obj)

    def _escribir_final(self, v):
        dt_primera = datetime.fromtimestamp(v.ts_primera)
        dt_ultima = datetime.fromtimestamp(v.ultimo_avistamiento)
        rssi_max = max(v.lista_rssi) if v.lista_rssi else 0
        rssi_prom = sum(v.lista_rssi) / len(v.lista_rssi) if v.lista_rssi else 0
        
        fila = [
            dt_primera.strftime('%Y-%m-%d'),
            dt_primera.strftime('%H:%M:%S.%f')[:-3],
            dt_ultima.strftime('%H:%M:%S.%f')[:-3],
            v.epc,
            v.puerto,
            v.contador_lecturas,
            rssi_max,
            round(rssi_prom, 2)
        ]
        with open(self.archivo_log, "a", newline='') as f:
            csv.writer(f).writerow(fila)
        print(">>> GUARDADO EN EXCEL: " + v.epc + " (" + v.puerto + ")")

    def stop(self):
        self.running = False
        for s in self.serials:
            try: s.write(CMD_STOP); s.close()
            except: pass

if __name__ == "__main__":
    sistema = GestorSigmaIndependiente()
    if sistema.start():
        try:
            while True: time.sleep(1)
        except KeyboardInterrupt:
            sistema.stop()