# Airflow_Tuts
Learning Apache Airflow for managing ETLs using Docker

# Pull Docker-Compose File

Run the curl command `curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'`
# Initialize Airflow
Run the docker command `docker-compose airlfow-init`

# Spin up the Project 

Run the command `docker-compose up`

# Concepts in this Repo
1. **TaskGroups**- Run parallel tasks as a group. A substitute to SUBDAGS. Find the implementation in `parallel_dag.py`
2. **TriggerDagOperator**- Trigger a secondary dag from a primary dag and wait for secondary dag to finish.
This can be implemented in a large project where there can be a master dag and subsidiary dags are triggered by the master DAG.

3. **Trigger Rules**- Changing the default trigger rule incase of a BranchPythonOperator. By default, all upstream tasks should be executed but since the BranchPythonOperator only executes one, the rule should be `TriggerRule.ONE_SUCCESS`