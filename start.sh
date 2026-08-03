#!/bin/bash
echo "Starting GSE Fleet Data Platform services..."
docker-compose up -d

echo "Waiting for services to initialize..."
sleep 10

echo "Current status:"
docker ps

echo "Done. Kafka should be available at localhost:9092"
