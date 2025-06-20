#!/bin/bash

echo "Stopping all services..."

# Stop the services
docker-compose down

echo
echo "All services have been stopped."
echo