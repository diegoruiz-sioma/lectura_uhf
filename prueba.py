# -- coding: utf-8 --
import serial
import serial.tools.list_ports
import time
import threading
import os
import csv
from datetime import datetime
from queue import Queue, Full

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

        # NEW
        self._q = Queue(maxsize=5000)

        if not os.path.exists(self.archivo_log):
            with open(self.archivo_log, "a", newline='') as f:
                csv.writer(f).writerow(["Fecha", "Hora_MS", "Tag_ID", "Puerto", "RSSI_dBm"])

    @staticmethod
    def _calc_checksum(frame: bytes) -> int:
        return sum(frame[1:-2]) & 0xFF

    def start(self):
        puertos = [p.device for p in serial.tools.list_ports.comports() if 'ttyUSB' in p.device]
        if not puertos:
            print(">>> ERROR: No se detectan sensores YRM1001 en los puertos USB.")
            return False

        self.running = True

        # NEW worker
        threading.Thread(target=self._worker_guardar, daemon=True).start()

        for p in puertos:
            t = threading.Thread(target=self._hilo_lector, args=(p,), daemon=True)
            t.start()

        print(f">>> SISTEMA ACTIVO: Capturando datos en {self.archivo_log}")
        print(">>> Presiona Ctrl+C para detener y guardar.")
        return True

    def _hilo_lector(self, puerto):
        try:
            ser = serial.Serial(puerto, 115200, timeout=0.05)
            self.seriales.append(ser)

            for cmd in [CMD_REGION, CMD_FREQ, CMD_POTENCIA, CMD_LEER]:
                ser.write(cmd)
                time.sleep(0.1)

            buffer = bytearray()

            while self.running:
                chunk = ser.read(ser.in_waiting or 1)
                if chunk:
                    buffer.extend(chunk)

                while True:
                    ini = buffer.find(b'\xBB')
                    if ini < 0:
                        buffer.clear()
                        break
                    if ini > 0:
                        del buffer[:ini]

                    if len(buffer) < 5:
                        break

                    pl = (buffer[3] << 8) | buffer[4]
                    frame_len = 7 + pl
                    if len(buffer) < frame_len:
                        break

                    trama = bytes(buffer[:frame_len])
                    del buffer[:frame_len]

                    if trama[-1] != 0x7E:
                        continue

                    if self._calc_checksum(trama) != trama[-2]:
                        continue

                    # Solo Notice + Cmd 0x22 = tag leído
                    if trama[1] == 0x02 and trama[2] == 0x22:
                        try:
                            self._q.put_nowait((trama, puerto))
                        except Full:
                            pass

        except Exception as e:
            print(f"Error en puerto {puerto}: {e}")

    def _worker_guardar(self):
        while self.running or not self._q.empty():
            try:
                trama, puerto = self._q.get(timeout=0.2)
            except:
                continue
            try:
                self._guardar_dato(trama, puerto)
            finally:
                self._q.task_done()

    def _guardar_dato(self, trama, puerto):
        ahora = datetime.now()

        pl = (trama[3] << 8) | trama[4]
        params = trama[5:5+pl]

        rssi_raw = params[0]
        rssi_dbm = rssi_raw - 256 if rssi_raw > 127 else rssi_raw

        # EPC = params[3:15] cuando PL=0x11
        tag_id = params[3:3+12].hex().upper()

        h_ms = ahora.strftime('%H:%M:%S.%f')[:-3]

        with self.lock:
            with open(self.archivo_log, "a", newline='') as f:
                csv.writer(f).writerow([ahora.strftime('%Y-%m-%d'), h_ms, tag_id, puerto, rssi_dbm])

        print(f"[{h_ms}] TAG: {tag_id} | POTENCIA: {rssi_dbm} dBm | puerto: {puerto}")    

    def stop(self):
        self.running = False
        try:
            self._q.join()
        except:
            pass

        for s in self.seriales:
            try:
                s.write(CMD_STOP)
                s.close()
            except:
                pass
        print("\n>>> Captura finalizada. Archivo guardado.")

if __name__ == "__main__":
    app = LectorRFID_dBm()
    if app.start():
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            app.stop()