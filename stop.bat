@echo off
echo Stopping all services...

REM Stop the services
docker-compose down

echo.
echo All services have been stopped.
echo.