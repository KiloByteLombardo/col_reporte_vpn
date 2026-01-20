# Imagen base de Python
FROM python:3.11-slim

# Variables de entorno
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Directorio de trabajo
WORKDIR /app

# Copiar requirements e instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY src/ ./src/

# Crear carpeta de resultados
RUN mkdir -p results

# Exponer puerto
EXPOSE 5000

# Directorio de trabajo para la app
WORKDIR /app/src

# Comando para ejecutar la aplicación
CMD ["python", "api.py"]

