@echo off
echo Starting Lakehouse ETL deployment...

echo Building Docker images...
docker build -f docker/Dockerfile.spark -t lakehouse-etl:latest .

echo Deployment completed!
pause