# Data Engineer Challenge
​
## Descripción General
Bienvenido a la solución del desafío para Ingenieros de Datos. Este repositorio contiene la solución a cada pregunta, y consideraciones adicionales que se deben tener, este desafío demuestra habilidades y conocimientos en procesamiento de datos utilizando Python, SQL, Spark ademas de versionamiento de software con Gitlow, entre otras.
​
## Objetivos
El objetivo principal de este desafío es resolver tres problemas específicos utilizando dos enfoques para cada uno: uno optimizado para el tiempo de ejecución y otro optimizado para el uso de memoria. Los problemas a resolver son:

1. Top 10 fechas con más tweets y el usuario con más publicaciones en esos días.
2. Top 10 emojis más usados con su respectivo conteo.
3. Top 10 usuarios más influyentes basado en el conteo de menciones (@).

## Soluciones
* Optimización de Tiempo de Ejecución:
Se utilizó un abordaje clásico con la librería time para medir el tiempo de ejecución de las funciones.
* Optimización de Uso de Memoria:
Se utilizó la librería memory-profiler para medir y optimizar el uso de memoria en las funciones.

## Resultados
Los resultados obtenidos se han documentado en el Jupyter Notebook, donde se muestran las siguientes métricas:

- Tiempo de Ejecución: Medido con la librería time.
- Uso de Memoria: Medido con la librería memory-profiler.
- Resultados al lanzar los procesos en Github Actions

## Buenas Prácticas
Se ha seguido una estructura clara y modular en el código, con buenas prácticas de GitFlow, manteniendo ramas de desarrollo separadas y una rama main para la versión final.


![imagen.png](../img/gitflow.png)

**SOLO LECTURA POR ACCESOS DE USUARIO**
[Challenge en Producción alojado en Colab Lectura](https://colab.research.google.com/drive/10ie1QSzcOrIfG2zRUEcJLhp71X74NVQE)

**IMPORTANTE: Los valores descritos en `challenge.ipynb` correspondientes a Github Actions se alinean a los ejecutas en la rama `Develop`. El lector mediante su respectivo inicio de sesión en sucuenta de Github podrá revisar los logs dados en ambientes de Develop y Main.**
