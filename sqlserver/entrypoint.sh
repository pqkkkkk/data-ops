#!/bin/bash

# Start SQL Server in the background
/opt/mssql/bin/sqlservr &

# Wait for SQL Server to start up
echo "Waiting for SQL Server to start..."
sleep 30

# Run the restore script
echo "Starting database restore..."
/tmp/restore_db.sh

# Check if restore was successful
if [ $? -eq 0 ]; then
    echo "Database restored successfully!"
else
    echo "Database restore failed!"
fi

# Keep the container running by waiting for SQL Server process
wait
