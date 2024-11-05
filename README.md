
# Setup

1. Build and Run Containers  

In the root directory, build and start all containers, including PostgreSQL, pgAdmin, and dbt:
docker-compose up -d --build

2. Access pgAdmin  

Navigate to http://localhost:5050 in your browser.  
Login credentials (configured in docker-compose.yml):  
- Username: fire_incidents@email.com  
- Password: admin123

3. Add Server in pgAdmin  

In pgAdmin, click Add New Server and configure as follows:  
- Name: fire_db_server  
- Host: postgres (matches the service name in docker-compose.yml)  
- Port: 5432  
- Username: db_user  
- Password: db_password  
Save and connect to view the available databases and schemas.

4. Install Python Requirements 

Access the dbt container shell to install required Python packages:  
docker exec -it fire_incidents-dbt-1 sh  
pip install -r /app/scripts/requirements.txt

5. Load Historical Data  

Run the script to load historical data from the source API:
python /app/scripts/load_data_from_api.py

6. Create Fire Incidents Model with dbt  

Access the dbt container shell and run dbt commands to create the model:
docker exec -it fire_incidents-dbt-1 sh  
cd dbt_project  
dbt run

7. Load Daily Data (Requires fire_incidents model)  

Run the script to load daily updates:
python /app/scripts/daily_load.py


----------------------------------------------------------------------------------------------

>>>>>>> e4fbeed (Initial commit)
