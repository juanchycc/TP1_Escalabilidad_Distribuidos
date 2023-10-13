# **Trabajo Práctico 1** - Sistemas Distribuidos - FIUBA
## **Alumnos:** Axel Kelman, Juan Cruz Caserío

---

## Requisitos:

Para poder ejecutar correctamente el trabajo, se deberá contar con la última versión de **Python**, **Docker** y **docker compose**.

---


## Ejecución del sistema:

* **Servidor**: Dentro de la carpeta del proyecto ejecutar el siguiente script, definiendo para cada componente la cantidad de nodos a desplegar a partir de sus flags:
```
./run.sh -q1 <#nodos> -q2 <#nodos> -q3 <#nodos> -avg <#nodos> -Mavg <#nodos> -q4 <#nodos>
```

* Los flags corresponden a los componentes:

    - q1: flights_filter
    - q2: flights_filter_distance
    - q3: flights_max
    - avg: filghts_avg
    - Mavg: flights_mayor_avg
    - q4: flughts_avg_by_journey

* **Cliente**: Desde la carpeta root dirigirse a la carpeta del cliente:

```
cd client
```
Se espera que en esta carpeta se encuentren los archivos flights.csv y airports.csv, los cuales contienen la información a ser enviada.
Una vez en la carpeta del cliente y el Servidor activo para mandar los paquetes ejecutar:

```
python3 ./main.py
```

---

## Finalización del sistema:

* **Servidor**: Para poder detener la ejecución de todos los nodos ejecutar el script:
```
./stop.sh
```
