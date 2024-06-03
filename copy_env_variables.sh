#!/bin/bash

# Archivo de salida para las variables de entorno
output_file="env_variables.txt"

# Limpiar el archivo de salida si ya existe
> $output_file

# Recorrer todas las variables de entorno y guardarlas en el archivo
for var in $(printenv)
do
    echo $var >> $output_file
done

echo "Variables de entorno copiadas a $output_file"
