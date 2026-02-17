# -*- coding: utf-8 -*-
import serial
import serial.tools.list_ports
import time
import threading
import os
import csv
from datetime import datetime

# --- COMANDOS ESTÁNDAR YRM1001 (Basados en tus documentos) ---
CMD_POTENCIA = bytes.fromhex("BB 00 B6 00 02 0A 28 EA 7E") # 26dBm
CMD_REGION   = bytes.fromhex("BB 00 07 00 01 02 0A 7E")    # US Region
CMD_FREQ     = bytes.fromhex("BB 00 AB 00 01 1A C6 7E")    # Canal RF
CMD_LEER     = bytes.fromhex("BB 00 27 00 03 22 FF FF 4A 7E") # Lectura Continua
CMD_STOP     = bytes.fromhex("BB 00 28 00 00 28 7E")

class LectorRFID_dBm:
    def __init__(self):
        self.archivo_log = "reporte_dbm_real.csv"
        self.lock = threading.Lock()
        self.running = False
        self.seriales = []

        # Crear archivo con cabecera si no existe
        if not os.path.exists(self.archivo_log):
            with open(self.archivo_log, "a", newline='') as f:
                csv.writer(f).writerow(["Fecha", "Hora_MS", "Tag_ID", "Puerto", "RSSI_dBm"])

    def start(self):
        puertos = [p.device for p in serial.tools.list_ports.comports() if 'ttyUSB' in p.device]
        if not puertos:
            print(">>> ERROR: No se detectan sensores YRM1001 en los puertos USB.")
            return False
        
        self.running = True
        for p in puertos:
            t = threading.Thread(target=self._hilo_lector, args=(p,), daemon=True)
            t.start()
        
        print(f">>> SISTEMA ACTIVO: Capturando datos en {self.archivo_log}")
        print(">>> Presiona Ctrl+C para detener y guardar.")
        return True

    def _hilo_lector(self, puerto):
        try:
            ser = serial.Serial(puerto, 115200, timeout=0.01)
            self.seriales.append(ser)
            
            # Inicializar sensor
            for cmd in [CMD_REGION, CMD_FREQ, CMD_POTENCIA, CMD_LEER]:
                ser.write(cmd)
                time.sleep(0.1)

            buffer = bytearray()
            while self.running:
                if ser.in_waiting:
                    buffer.extend(ser.read(ser.in_waiting))
                    while True:
                        ini = buffer.find(b'\xBB')
                        if ini < 0: 
                            buffer.clear()
                            break
                        if ini > 0: 
                            del buffer[:ini]
                        
                        fin = buffer.find(b'\x7E', 1)
                        if fin < 0: break
                        
                        trama = bytes(buffer[:fin + 1])
                        del buffer[:fin + 1]
                        
                        # Procesar solo tramas de lectura (Comando 0x22)
                        if len(trama) >= 21 and trama[2] == 0x22:
                            self._guardar_dato(trama, puerto)
                else:
                    time.sleep(0.001)
        except Exception as e:
            print(f"Error en puerto {puerto}: {e}")

    def _guardar_dato(self, trama, puerto):
        ahora = datetime.now()
        
        # 1. Extraer EPC (ID del Tag) - Basado en manual
        # El manual indica que el EPC empieza en el byte 8 (después de Header, Type, Cmd, Len, y 3 bytes de control)
        # Pero para ser seguros con el YRM1001, tomamos los 12 bytes centrales.
        tag_id = trama[8:20].hex().upper()
        
        # 2. Extraer RSSI (Penúltimo byte antes del Checksum y Final)
        # Según tu manual: [EPC] [CRC_Tag] [RSSI] [Checksum] [7E]
        # Por tanto, el RSSI está en la posición -3
        rssi_raw = trama[-3]
        
        # 3. Lógica de Complemento a 2 (Literal del Datasheet)
        if rssi_raw > 127:
            rssi_dbm = rssi_raw - 256
        else:
            rssi_dbm = rssi_raw

        h_ms = ahora.strftime('%H:%M:%S.%f')[:-3]
        
        # 4. Escritura en CSV
        with self.lock:
            with open(self.archivo_log, "a", newline='') as f:
                csv.writer(f).writerow([ahora.strftime('%Y-%m-%d'), h_ms, tag_id, puerto, rssi_dbm])
        
        print(f"[{h_ms}] TAG: {tag_id} | POTENCIA: {rssi_dbm} dBm")

    def stop(self):
        self.running = False
        for s in self.seriales:
            try:
                s.write(CMD_STOP)
                s.close()
            except: pass
        print("\n>>> Captura finalizada. Archivo guardado.")

if __name__ == "__main__":
    app = LectorRFID_dBm()
    if app.start():
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            app.stop()