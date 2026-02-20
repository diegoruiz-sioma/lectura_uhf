# -*- coding: utf-8 -*-
import csv
import os
from datetime import datetime

def filtrar_por_viajes(archivo_origen, archivo_destino, minutos_nuevo_viaje=1):
    if not os.path.exists(archivo_origen):
        print("Error: No se encuentra " + archivo_origen)
        return

    ultimas_vistas = {} # Para saber hace cuanto vimos cada tag
    lista_final = []

    with open(archivo_origen, 'r', encoding='latin-1') as f:
        lector = csv.DictReader(f)
        for fila in lector:
            try:
                tag = fila['Tag_ID']
                tiempo_actual = datetime.strptime(fila['Fecha'] + " " + fila['Hora_MS'], "%Y-%m-%d %H:%M:%S.%f")
                
                # REGLA PARA NUEVO VIAJE:
                # Si el tag es nuevo O si han pasado mas de X minutos desde la ultima vez
                if tag not in ultimas_vistas:
                    lista_final.append(fila)
                    print("Tag detectado (Primer viaje): " + tag)
                else:
                    diferencia = (tiempo_actual - ultimas_vistas[tag]).total_seconds()
                    # Si pasaron mas de 60 segundos (1 minuto), es un viaje nuevo
                    if diferencia > (minutos_nuevo_viaje * 60):
                        lista_final.append(fila)
                        print("Tag detectado (Nuevo viaje): " + tag)
                
                # Actualizamos siempre la ultima vez que lo vimos
                ultimas_vistas[tag] = tiempo_actual
            except:
                continue 

    if lista_final:
        with open(archivo_destino, 'w', newline='', encoding='utf-8') as f:
            columnas = ["Fecha", "Hora_MS", "Tag_ID", "Puerto", "RSSI_dBm"]
            escritor = csv.DictWriter(f, fieldnames=columnas, extrasaction='ignore')
            escritor.writeheader()
            escritor.writerows(lista_final)
        
        print("\n" + "="*40)
        print(" FILTRADO POR VIAJES COMPLETADO ")
        print("="*40)
        print("Total de registros guardados: " + str(len(lista_final)))
    else:
        print("No hay datos nuevos.")

if __name__ == "__main__":
    # Cambia el '1' por los minutos que sueles tardar entre viaje y viaje
    filtrar_por_viajes("reporte_dbm_real.csv", "orden_entrada_tags.csv", minutos_nuevo_viaje=1)