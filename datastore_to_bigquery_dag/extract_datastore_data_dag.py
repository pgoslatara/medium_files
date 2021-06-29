from datetime import datetime
from airflow import DAG, settings
from airflow.models import Connection
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.datastore import CloudDatastoreExportEntitiesOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python_operator import PythonOperator
import json, os

def create_conn(**kwargs):
	project=kwargs["templates_dict"]["project"]

	new_conn=Connection(
		conn_id=f"datastore_conn_{project}",
		conn_type="Google Cloud Platform"
	)
	conn_extra={
		"extra__google_cloud_platform__project": project
	}
	conn_extra_json=json.dumps(conn_extra)
	new_conn.set_extra(conn_extra_json)

	session=settings.Session()

	# Create connection if not already exists
	if session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).first():
		pass
	else:
		session.add(new_conn)
		session.commit()

entities_to_extract=[
	{
		"source_project": "project-1",
		"entities": ["entity_1", "entity_2"]
	},
	{
		"source_project": "project-2",
		"entities": ["entity_3"]
	},
]

default_args={
	"catchup": False,
	"max_active_runs": 1,
	"owner": "airflow",
	"retries": 2,
	"start_date": datetime(2021, 6, 28),
	"use_legacy_sql": False,
	"write_disposition": "WRITE_TRUNCATE",
}

BUCKET=os.environ["GCS_BUCKET"]

dag=DAG(
	dag_id="extract_datastore_data",
	default_args=default_args,
	schedule_interval="0 0 * * *")

for obj in entities_to_extract:

	PROJECT=obj["source_project"]

	create_datastore_connection=PythonOperator(
		task_id=f"create_datastore_connection_{PROJECT}",
		python_callable=create_conn,
		templates_dict={
			"project": PROJECT
		},
		provide_context=True,
		dag=dag)

	grant_sa_access_to_gcs=BashOperator(
		task_id=f"grant_sa_access_to_gcs_{PROJECT}",
		bash_command=f"gsutil iam ch serviceAccount:{PROJECT}@appspot.gserviceaccount.com:admin gs://{BUCKET}/",
		dag=dag)

	download_to_gcs=CloudDatastoreExportEntitiesOperator(
		task_id=f"download_to_gcs_{PROJECT}",
		datastore_conn_id=f"datastore_conn_{PROJECT}",
		bucket=BUCKET,
		namespace=f"data/datastore_data/{PROJECT}/{{{{ ds_nodash }}}}",
		entity_filter={"kinds": obj["entities"]},
		overwrite_existing=True,
		dag=dag)

	[create_datastore_connection,grant_sa_access_to_gcs]>>download_to_gcs

	for entity in obj["entities"]:

		DESTINATION_BQ_DATASET=f"datastore_{PROJECT.replace('-','_')}"

		load_gcs_to_bq=GCSToBigQueryOperator(
			task_id=f"load_gcs_to_bq_{entity}",
			bucket=BUCKET,
			source_objects=[f"data/datastore_data/{PROJECT}/{{{{ ds_nodash }}}}/all_namespaces/kind_{entity}/all_namespaces_kind_{entity}.export_metadata"],
			source_format="DATASTORE_BACKUP",
			destination_project_dataset_table=f"{DESTINATION_BQ_DATASET}.{entity}",
			create_disposition="CREATE_IF_NEEDED",
			dag=dag)

		download_to_gcs>>load_gcs_to_bq
