#!/bin/bash

echo "Starting HAPI FHIR Server..."

# Start HAPI FHIR in background
java -jar /app/main.jar &
HAPI_PID=$!

# Wait for HAPI FHIR to be ready
echo "Waiting for HAPI FHIR to start..."
for i in {1..60}; do
    if curl -s http://localhost:8080/fhir/metadata > /dev/null 2>&1; then
        echo "HAPI FHIR is ready!"
        break
    fi
    if [ $i -eq 60 ]; then
        echo "Timeout waiting for HAPI FHIR to start"
        exit 1
    fi
    sleep 2
done

# Load custom search parameters
if [ -f /app/config/custom-search-parameters.json ]; then
    echo "Loading custom search parameters..."
    curl -X POST \
        -H "Content-Type: application/fhir+json" \
        -d @/app/config/custom-search-parameters.json \
        http://localhost:8080/fhir
    
    if [ $? -eq 0 ]; then
        echo "Custom search parameters loaded successfully"
    else
        echo "Warning: Failed to load custom search parameters"
    fi
else
    echo "No custom search parameters file found"
fi

# Reindex to apply search parameters
echo "Triggering reindex for DocumentReference..."
curl -X POST \
    -H "Content-Type: application/fhir+json" \
    -d '{"resourceType":"Parameters","parameter":[{"name":"url","valueString":"http://hapi-fhir.au/SearchParameter/DocumentReference-content"}]}' \
    http://localhost:8080/fhir/\$reindex

echo "Initialization complete"

# Keep the container running by waiting for the Java process
wait $HAPI_PID
