# -*- coding: utf-8 -*-
import serial
import serial.tools.list_ports
import time
import threading
import os
import csv
from datetime import datetime

# --- CONFIGURACIÓN DE PROTOCOLO HARDWARE ---
CMD_POTENCIA_26DBM = bytes.fromhex("BB 00 B6 00 02 0A 28 EA 7E") 
CMD_LECTURA = bytes.fromhex("BB 00 27 00 03 22 FF FF 4A 7E")
CMD_REGION_US = bytes.fromhex("BB 00 07 00 01 02 0A 7E")
CMD_915_MHZ = bytes.fromhex("BB 00 AB 00 01 1A C6 7E")

class RegistradorSigma:
    def __init__(self):
        self.archivo_log = "reporte_limpio.csv"
        self.tags_en_escena = {}  
        self.bloqueo_temporal = {} 
        self.candado = threading.Lock()
        
        # --- PARÁMETROS DE OPERACIÓN ---
        self.umbral_latencia_lote = 1.2   # Ventana de tiempo para cierre de secuencia
        self.tiempo_bloqueo = 600        # Intervalo de exclusión (segundos)
        self.min_lecturas_validacion = 1 
        self.ultimo_evento_detectado = time.time()
        
        self._running = False
        self._serials = []
        self.escribir_csv = True
        self.imprimir_eventos = True

        if not os.path.exists(self.archivo_log):
            self._inicializar_archivo()

    def _inicializar_archivo(self):
        with open(self.archivo_log, "a", newline='') as f:
            csv.writer(f).writerow(["Fecha", "Hora", "Tag_ID", "Lecturas"])

    def start(self):
        puertos = [p.device for p in serial.tools.list_ports.comports() if 'ttyUSB' in p.device]
        if not puertos:
            if self.imprimir_eventos:
                print("------------------------------------------")
                print("SIGMA UHF - Error: No hay sensores")
                print("------------------------------------------")
            return False
        
        self._running = True
        num_sensores = len(puertos)

        # Inicialización de hilos de control y monitoreo
        threading.Thread(target=self._procesamiento_secuencial_lotes, daemon=True).start()
        
        for p in puertos:
            t = threading.Thread(target=self._gestion_interfaz_serial, args=(p,), daemon=True)
            t.start()
        
        if self.imprimir_eventos:
            print("------------------------------------------")
            print("SIGMA UHF")
            print(f"Sensores conectados: {num_sensores}")
            print("------------------------------------------")

        return True

    def _gestion_interfaz_serial(self, puerto):
        """Gestión de bajo nivel para la captura de tramas UHF."""
        try:
            ser = serial.Serial(puerto, 115200, timeout=0.01)
            self._serials.append(ser)
            
            # Inicialización de hardware
            ser.write(CMD_REGION_US); time.sleep(0.1)
            ser.write(CMD_915_MHZ); time.sleep(0.1)
            ser.write(CMD_POTENCIA_26DBM); time.sleep(0.1)
            ser.write(CMD_LECTURA)

            buffer_circular = bytearray()
            while self._running:
                if ser.in_waiting:
                    buffer_circular.extend(ser.read(ser.in_waiting))
                    while True:
                        inicio = buffer_circular.find(b'\xBB')
                        if inicio < 0:
                            buffer_circular.clear()
                            break
                        if inicio > 0:
                            del buffer_circular[:inicio]
                        
                        fin = buffer_circular.find(b'\x7E', 1)
                        if fin < 0:
                            break
                        
                        trama = bytes(buffer_circular[:fin + 1])
                        del buffer_circular[:fin + 1]
                        self._analisis_discriminatorio_tag(trama)
                else:
                    time.sleep(0.001)
        except Exception:
            pass

    def _analisis_discriminatorio_tag(self, trama):
        """Extrae el identificador y registra el timestamp de detección inicial."""
        if len(trama) >= 19 and trama[1] == 0x02:
            segmento = trama[2:-1] if trama[-1] == 0x7E else trama[2:]
            tag_id = segmento[6:18].hex().upper()
            ahora = time.time()
            
            with self.candado:
                self.ultimo_evento_detectado = ahora
                if tag_id in self.bloqueo_temporal:
                    return
                
                if tag_id not in self.tags_en_escena:
                    # Registro de telemetría inicial para ordenamiento cronológico
                    self.tags_en_escena[tag_id] = {
                        'ts_inicial': ahora,
                        'conteo': 1
                    }
                else:
                    self.tags_en_escena[tag_id]['conteo'] += 1

    def _procesamiento_secuencial_lotes(self):
        """Aplica lógica de ordenamiento tras detectar fin de ráfaga de datos."""
        while self._running:
            time.sleep(0.2)
            ahora = time.time()
            
            with self.candado:
                # Se activa el procesamiento si se supera el umbral de silencio
                if self.tags_en_escena and (ahora - self.ultimo_evento_detectado > self.umbral_latencia_lote):
                    
                    # Generación de matriz para ordenamiento
                    lote_ordenado = list(self.tags_en_escena.items())
                    
                    # Algoritmo de ordenamiento basado en detección de primer flanco
                    lote_ordenado.sort(key=lambda x: x[1]['ts_inicial'])
                    
                    for tid, data in lote_ordenado:
                        self._persistencia_datos(tid, data)
                        self.bloqueo_temporal[tid] = ahora + self.tiempo_bloqueo
                    
                    self.tags_en_escena.clear()

    def _persistencia_datos(self, tid, data):
        """Escritura de registros validados en almacenamiento persistente."""
        ts = datetime.fromtimestamp(data['ts_inicial'])
        if self.escribir_csv:
            with open(self.archivo_log, "a", newline='') as f:
                csv.writer(f).writerow([
                    ts.strftime('%Y-%m-%d'), 
                    ts.strftime('%H:%M:%S.%f')[:-3], 
                    tid, 
                    data['conteo']
                ])
        
        if self.imprimir_eventos:
            print(f"ID: {tid} | REGISTRADO | {ts.strftime('%H:%M:%S.%f')[:-3]}")

    def stop(self):
        self._running = False
        for s in self._serials:
            try:
                s.close()
            except:
                pass

if __name__ == "__main__":
    app = RegistradorSigma()
    app.imprimir_eventos = True
    app.escribir_csv = True

    if app.start():
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            app.stop()