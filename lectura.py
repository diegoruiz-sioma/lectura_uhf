# -*- coding: utf-8 -*-
import serial
import serial.tools.list_ports
import time
import threading
import os
import csv
from datetime import datetime

# --- CONFIGURACION DE HARDWARE ---
CMD_POTENCIA_26DBM = bytes.fromhex("BB 00 B6 00 02 0A 28 EA 7E") 
CMD_LECTURA = bytes.fromhex("BB 00 27 00 03 22 FF FF 4A 7E")
CMD_REGION_US = bytes.fromhex("BB 00 07 00 01 02 0A 7E")
CMD_915_MHZ = bytes.fromhex("BB 00 AB 00 01 1A C6 7E")
CMD_STOP_LECTURAS = bytes.fromhex("BB 00 28 00 00 28 7E")

class RegistradorSigma:
    def __init__(self):
        self.archivo_log = "reporte_limpio.csv"
        self.tags_en_escena = {}  
        self.bloqueo_temporal = {} 
        
        # --- AJUSTES DE TIEMPO ---
        self.umbral_salida = 0.8    
        self.tiempo_bloqueo = 600   # Bloqueo de 10 minutos
        self.min_lecturas = 2     
        
        self.candado = threading.Lock()
        self.sensores_conectados = 0
        
        self._running = False                # bandera control ciclo de vida
        self._threads = []                   # lista de hilos creados
        self._serials = []                   # lista de objetos serial abiertos
        self._buffers = {}                   # buffer por puerto

        self.escribir_csv = False             # por defecto mantiene comportamiento actual
        self.imprimir_eventos = False         # por defecto mantiene comportamiento actual

        # Hook de integración: callback/event queue (se usa más adelante)
        self.on_event = None                 # callable(event_dict)
        self.event_queue = None              # queue.Queue

        if self.escribir_csv and (not os.path.exists(self.archivo_log)):
            with open(self.archivo_log, "a", newline='') as f:
                csv.writer(f).writerow([
                    "Fecha", "Hora", "Tag_ID", "Total_Lecturas",
                    "Num_Sensores", "RSSI_Prom", "Latencia_MS"
                ])  

             

    def start(self):
        puertos = [p.device for p in serial.tools.list_ports.comports() if 'ttyUSB' in p.device]
        if not puertos:
            if self.imprimir_eventos:
                print("------------------------------------------")
                print("SIGMA UHF - Error: No hay sensores")
                print("------------------------------------------")
            self._running = False
            return False
        
        self.sensores_conectados = len(puertos)
        self._running = True
        
        # Hilos de control
        threadMonitor = threading.Thread(target=self._monitor_de_paso, daemon=True)
        threadMonitor.start()
        self._threads.append(threadMonitor)

        threadLimpiar = threading.Thread(target=self._limpiar_bloqueos, daemon=True)
        threadLimpiar.start()
        self._threads.append(threadLimpiar)
        
        for p in puertos:
            t = threading.Thread(target=self._leer_sensor, args=(p,), daemon=True)
            t.start()
            self._threads.append(t)
        
        # Encabezado solicitado con cantidad de sensores
        if self.imprimir_eventos:
            print("------------------------------------------")
            print("SIGMA UHF")
            print("Sensores conectados: " + str(self.sensores_conectados))
            print("------------------------------------------")

        return True

    def _leer_sensor(self, puerto):
        ser = None
        try:
            ser = serial.Serial(puerto, 115200, timeout=0.1)
            self._serials.append(ser)
            ser.write(CMD_REGION_US); time.sleep(0.1)
            ser.write(CMD_915_MHZ); time.sleep(0.1)
            ser.write(CMD_POTENCIA_26DBM); time.sleep(0.1)
            ser.write(CMD_LECTURA)

            if puerto not in self._buffers:
                self._buffers[puerto] = bytearray()

            buf = self._buffers[puerto]
            
            while self._running:
                chunk = ser.read(256) 

                if not chunk:
                    continue

                if chunk:
                    buf.extend(chunk)

                frames = self._extraer_frames(buf)
                ahora = time.time()
                for fr in frames:
                    tag_id, rssi_raw = self._frame_a_tag(fr)
                    if tag_id is None:
                        continue
                    
                    with self.candado:
                        if tag_id in self.bloqueo_temporal:
                            continue
                                
                        if tag_id not in self.tags_en_escena:
                            self.tags_en_escena[tag_id] = {
                                'inicio': ahora, 'ultimo': ahora, 'conteo': 1,
                                'sensores': {puerto}, 'rssi_total': rssi_raw
                                }
                        else:
                            self.tags_en_escena[tag_id]['ultimo'] = ahora
                            self.tags_en_escena[tag_id]['conteo'] += 1
                            self.tags_en_escena[tag_id]['sensores'].add(puerto)
                            self.tags_en_escena[tag_id]['rssi_total'] += rssi_raw
                
        except Exception as e:
            # Si estamos deteniendo el sistema, este error es normal
            if not self._running:
                return

            if self.imprimir_eventos:
                print("Error sensor:", e)

        
        finally:
            if ser is not None:
                try:
                    ser.close()
                except Exception:
                    pass
    

    def _monitor_de_paso(self):
        while self._running:
            ahora = time.time()
            finalizados = []
            
            with self.candado:
                for tag_id, info in list(self.tags_en_escena.items()):
                    if (ahora - info['ultimo']) > self.umbral_salida:
                        if info['conteo'] >= self.min_lecturas:
                            finalizados.append((tag_id, info))
                        del self.tags_en_escena[tag_id]

            for tag_id, info in finalizados:
                with self.candado:
                    # Bloqueo activo para evitar dobles lecturas
                    self.bloqueo_temporal[tag_id] = ahora + self.tiempo_bloqueo
                self._escribir_excel(tag_id, info)
            
            time.sleep(0.1)

    def _limpiar_bloqueos(self):
        while self._running:
            ahora = time.time()
            with self.candado:
                self.bloqueo_temporal = {tid: texp for tid, texp in self.bloqueo_temporal.items() if ahora < texp}
            time.sleep(10)

    def _escribir_excel(self, tag_id, info):
        dt = datetime.fromtimestamp(info['inicio'])
        latencia = (info['ultimo'] - info['inicio']) * 1000
        rssi_prom = round(info['rssi_total'] / float(info['conteo']), 2)
        
        datos = [
            dt.strftime('%Y-%m-%d'), dt.strftime('%H:%M:%S.%f')[:-3], 
            tag_id, info['conteo'], len(info['sensores']), rssi_prom, round(latencia, 2)
        ]
        
        try:
            if self.escribir_csv:
                with open(self.archivo_log, "a", newline='') as f:
                    csv.writer(f).writerow(datos)

            if self.imprimir_eventos:
                print("NUEVO TAG DETECTADO: " + str(tag_id))

        except Exception as e:
            if self.imprimir_eventos:
                print("Error Excel: " + str(e))
    
    def _extraer_frames(self, buf: bytearray):
        """
        Extrae frames completos delimitados por:
        start: 0xBB
        end:   0x7E
        Devuelve lista de frames (bytes). Modifica buf in-place (consume lo extraído).
        """
        frames = []

        while True:
            start = buf.find(b'\xBB')
            if start < 0:
                # no hay inicio: deja el buffer vacío (o conserva últimos bytes si quieres)
                buf.clear()
                break

            if start > 0:
                # descarta basura antes del inicio
                del buf[:start]

            end = buf.find(b'\x7E', 1)
            if end < 0:
                # frame incompleto: esperar más datos
                break

            frame = bytes(buf[:end + 1])
            frames.append(frame)
            del buf[:end + 1]

        return frames

    def _frame_a_tag(self, frame: bytes):
        """
        Dado un frame completo: BB 02 ... 7E
        extrae tag_id y rssi_raw siguiendo EXACTAMENTE tu lógica actual.

        Retorna: (tag_id:str, rssi_raw:int) o (None, None) si no aplica.
        """
        # Frame mínimo: BB 02 ... 7E
        if frame is None or len(frame) < 4:
            return None, None

        if frame[0] != 0xBB or frame[1] != 0x02:
            return None, None

        # Quitar prefijo BB 02 y cola 7E (si está)
        if frame[-1] == 0x7E:
            p = frame[2:-1]
        else:
            p = frame[2:]

        # Tu condición actual
        if len(p) < 19:
            return None, None

        # Tu extracción exacta
        tag_id = p[6:18].hex().upper()

        # Tu RSSI exacto (penúltimo byte del fragmento p)
        rssi_raw = p[-2] if len(p) > 18 else 0

        return tag_id, rssi_raw


    
    def _emit_event(self, event: dict):
        """
        Emite eventos para integración sin forzar side-effects.
        Se activará en pasos posteriores.
        """
        try:
            if callable(self.on_event):
                self.on_event(event)
            if self.event_queue is not None:
                self.event_queue.put(event)
        except Exception as e:
            # Nunca debe tumbar el lector por un error del integrador
            if getattr(self, "imprimir_eventos", False):
                print("Error emit_event:", e)

    def stop(self):
        """
        Detiene el módulo de forma segura (se completará en pasos posteriores).
        Por ahora solo baja la bandera y cierra seriales si existen.
        """
        self._running = False

        # Cerrar puertos serial si ya están abiertos
        for s in getattr(self, "_serials", []):
            try:
                s.close()
            except Exception:
                pass
    
    def join(self, timeout=None):
        for t in list(self._threads):
            try:
                t.join(timeout=timeout)
            except Exception:
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
            print("Deteniendo monitoreo...")
            app.stop()
            app.join()
            print("Monitoreo detenido.")
