# Usar la imagen oficial de Python 3.11 ligera
FROM python:3.13-slim

# Establecer el directorio de trabajo en el contenedor
WORKDIR /workspace

# Copiar el archivo de dependencias primero para aprovechar la caché de Docker
COPY requeriments.txt .

# Instalar las dependencias necesarias
RUN pip install --no-cache-dir -r requeriments.txt

# Copiar el código del main.py al contenedor
COPY main.py .

# Exponer el puerto 8080 que es el estándar para Google Cloud Run
EXPOSE 8080

# Variable de entorno para el puerto
ENV PORT=8080

# Comando para ejecutar la función usando functions-framework
# --target=registro_clientes_online indica que esa es la función que debe iniciar
CMD exec functions-framework --target=registro_clientes_online --port=$PORT --host=0.0.0.0