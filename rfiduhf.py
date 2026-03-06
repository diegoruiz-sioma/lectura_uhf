
# -*- coding: utf-8 -*-

import serial
import time
import psycopg2

class RFIDUHF:

    def __init__(self):
        self.port = "/dev/ttyUSB0"
        self.baudrate = 115200
        self.ser = None
        self.last_viaje = None
        self.connect_reader()

    # ----------------------------------------
    # CONEXION LECTOR RFID
    # ----------------------------------------
    def connect_reader(self):
        try:
            self.ser = serial.Serial(self.port, self.baudrate, timeout=0.1)
            print("RFIDUHF conectado al lector")
        except Exception as e:
            print("Error conectando lector RFID:", e)

    # ----------------------------------------
    # CONEXION BASE DE DATOS
    # ----------------------------------------
    def db(self):
        return psycopg2.connect(
            dbname="estomadb",
            user="postgres",
            password="sioma"
        )

    # ----------------------------------------
    # CAPTURA CONTINUA RFID
    # ----------------------------------------
    def capturar(self):

        try:
            data = self.ser.read(self.ser.in_waiting or 1)

            if not data:
                return

            epc = data.hex().upper()

            # usar últimos 4 caracteres como ID
            tag = epc[-4:]

            with self.db() as conn:
                with conn.cursor() as cur:

                    cur.execute("""
                        INSERT INTO rfid_raw_reads
                        (fecha, epc_hex, tag_id)
                        VALUES (NOW(), %s, %s)
                    """, (epc, tag))

                conn.commit()

        except:
            pass

    # ----------------------------------------
    # OBTENER VIAJE ACTUAL
    # ----------------------------------------
    def get_viaje(self):

        with self.db() as conn:
            with conn.cursor() as cur:

                cur.execute("SELECT max(viaje_id) FROM viajes")
                row = cur.fetchone()

                return row[0]

    # ----------------------------------------
    # PROCESAR VIAJE FINALIZADO
    # ----------------------------------------
    def procesar_viaje(self, viaje_id):

        with self.db() as conn:
            with conn.cursor() as cur:

                cur.execute("""
                    SELECT fecha, fecha_final
                    FROM viajes
                    WHERE viaje_id=%s
                """, (viaje_id,))

                row = cur.fetchone()

                inicio = row[0]
                fin = row[1]

                # obtener lecturas RFID
                cur.execute("""
                    SELECT tag_id
                    FROM rfid_raw_reads
                    WHERE fecha >= %s AND fecha <= %s
                    ORDER BY fecha
                """, (inicio, fin))

                tags = [r[0] for r in cur.fetchall()]

                # eliminar duplicados manteniendo orden
                tags_orden = []
                for t in tags:
                    if t not in tags_orden:
                        tags_orden.append(t)

                # obtener racimos del viaje
                cur.execute("""
                    SELECT racimito_id
                    FROM racimitos
                    WHERE viaje_id=%s
                    ORDER BY racimito_id
                """, (viaje_id,))

                racimos = [r[0] for r in cur.fetchall()]

                # asignación tag -> racimo
                for i, r in enumerate(racimos):

                    if i < len(tags_orden):

                        cur.execute("""
                            UPDATE racimitos
                            SET serial=%s
                            WHERE racimito_id=%s
                        """, (tags_orden[i], r))

                    else:

                        cur.execute("""
                            UPDATE racimitos
                            SET serial=NULL
                            WHERE racimito_id=%s
                        """, (r,))

                conn.commit()

                # limpieza automática
                cur.execute("""
                    DELETE FROM rfid_raw_reads
                    WHERE fecha >= %s AND fecha <= %s
                """, (inicio, fin))

                conn.commit()

                print("Viaje procesado:", viaje_id)

    # ----------------------------------------
    # LOOP PRINCIPAL
    # ----------------------------------------
    def run(self):

        while True:

            try:

                self.capturar()

                viaje = self.get_viaje()

                if self.last_viaje is None:
                    self.last_viaje = viaje

                if viaje != self.last_viaje:

                    self.procesar_viaje(self.last_viaje)

                    self.last_viaje = viaje

            except Exception as e:
                print("RFIDUHF error:", e)

            time.sleep(0.2)
