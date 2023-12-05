# -*- coding: utf-8 -*-

import uuid
import pandas as pd
import random
import sys

def hacer_casos(input_file):
    # Cargar el archivo CSV
    primera = True
    with open(input_file, 'w') as aArchivo:
        aArchivo.write(f"legId,searchDate,flightDate,startingAirport,destinationAirport,fareBasisCode,travelDuration,elapsedDays,isBasicEconomy,isRefundable,isNonStop,baseFare,totalFare,seatsRemaining,totalTravelDistance,segmentsDepartureTimeEpochSeconds,segmentsDepartureTimeRaw,segmentsArrivalTimeEpochSeconds,segmentsArrivalTimeRaw,segmentsArrivalAirportCode,segmentsDepartureAirportCode,segmentsAirlineName,segmentsAirlineCode,segmentsEquipmentDescription,segmentsDurationInSeconds,segmentsDistance,segmentsCabinCode\n")

        for i in range(1, 10):
            
            guid = uuid.uuid4()
            id_vuelo = str(guid).replace("-", "")
            origen = "DFW"
            destino = "DEN"
            duracion = "PT2H6M"
            escala = "DEN, MIA"
            aArchivo.write(f"{id_vuelo},2022-04-19,2022-04-30,{origen},{destino},SAA1AKEN,{duracion},0,False,False,True,110.70,133.60,9,650,1651356240,2022-04-30T17:04:00.000-05:00,1651363800,2022-04-30T18:10:00.000-06:00,{escala},DFW,United,UA,Airbus A320,7560,650,coach\n")

if __name__ == "__main__":

    # Obtener argumentos de la linea de comandos
    archivo_entrada = sys.argv[1]

    # Ejecutar la funcion principal
    hacer_casos(archivo_entrada)
