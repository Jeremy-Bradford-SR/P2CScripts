# Stage 1: Build React UI
FROM node:18-alpine as ui-build
WORKDIR /app/ui
COPY orchestrator/ui/package.json ./
RUN npm install
COPY orchestrator/ui ./
RUN npm run build

# Stage 2: Python Runtime
FROM python:3.9-bullseye
WORKDIR /app

# Install System Dependencies and MS SQL Drivers
RUN apt-get update && apt-get install -y gnupg2 curl unixodbc unixodbc-dev
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Python Dependencies
RUN pip install fastapi uvicorn requests pyodbc python-dotenv beautifulsoup4

# Copy Application Code
COPY . .

# Copy Built UI from Stage 1
# Ensure the server looks for ui/dist relative to where it runs
# We copy it to orchestrator/ui/dist relative to WORKDIR
COPY --from=ui-build /app/ui/dist ./orchestrator/ui/dist

# Environment
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Create data directory for persistence
RUN mkdir -p /data

# Run Orchestrator
EXPOSE 8005
CMD ["python3", "-m", "orchestrator.server"]
