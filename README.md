### El programa es un ETL que hace lo siguiente:
* Preprocesa los datos de clientes con pandas y administra el uso de memoria
* Transforma los datos y crea 3 dataframes que se guardan en el archivo output en formato .xlsx
* Al correr el programa por primera vez crea una base de datos, crea las tablas y lo guarda en el sistema de archivos
* Crea las tablas en la base de datos con un tipo de dato indicado para cada variable de las tablas
* Almacena los datos de los 3 dataframes en la db
* Cuando se vuelve a ejecutar el programa los datos se guardaran en la db que quedo en el sistema de archivos

### Instrucciones:
* Ejecute el archivo setup.py
* El script le pedira que ingrese la ruta del archivo clientes.csv
* Si la ruta y el archivo es correcto ejecutara el programa
