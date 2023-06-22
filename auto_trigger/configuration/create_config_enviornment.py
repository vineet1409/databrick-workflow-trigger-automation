import requests
import time
import argparse

from auto_trigger.configuration.constants import config
from auto_trigger.logger.mlflow_logger import log_to_mlflow



def create_enviornment(instance_url,token,args):
    # Databricks REST API endpoint
    databricks_instance = "https://"+instance_url
    url = f"{databricks_instance}/api/2.0/clusters/list"

    # Set the request headers
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Send GET request to the clusters endpoint
    response = requests.get(url, headers=headers)
    cluster_list = []

    # Check if the request was successful
    if response.status_code == 200:
        clusters = response.json()["clusters"]
        for cluster in clusters:
            cluster_name = cluster["cluster_name"]
            if not cluster_name.startswith("job-"):
                cluster_list.append(cluster)
    else:
        print("Failed to retrieve clusters. Status code:", response.status_code)

    # Get detailed information for the first cluster in the list
    if len(cluster_list) > 0:
        first_cluster = cluster_list[0]
        cluster_id = first_cluster["cluster_id"]
        cluster_info_url = f"{databricks_instance}/api/2.0/clusters/get?cluster_id={cluster_id}"

        # Send GET request to the cluster info endpoint
        response = requests.get(cluster_info_url, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            cluster_info = response.json()

            if cluster_info["state"] == "TERMINATED":
                print('Cluster in terminated state, starting it first..This may take a few minutes..!!')
                restart_cluster_url = f"{databricks_instance}/api/2.0/clusters/start"
                start_cluster = requests.post(restart_cluster_url, headers=headers, json={"cluster_id": cluster_id})
                time.sleep(int(config['cluster_startup_wait_time'])) 
                if start_cluster.status_code == 200:
                    print('cluster started successfully')
                else:
                    print('failed to start cluster')

            # You can extract specific cluster details from the cluster_info dictionary
            print("Cluster Information:")
            print("Cluster ID:", cluster_info["cluster_id"])
            print("Cluster Name:", cluster_info["cluster_name"])
            print("Current Cluster Spark:", cluster_info["spark_version"])
            print("Current Cluster Min Workers:", cluster_info["autoscale"]["min_workers"])
            print("Current Cluster Max Workers:", cluster_info["autoscale"]["max_workers"])
            print("\n")
            current_cluster_config = {'Cluster ID':cluster_info["cluster_id"],
                                      'Cluster Name':cluster_info["cluster_name"],
                                      'Current Cluster Spark':cluster_info["spark_version"],
                                      'Current Cluster Min Workers':cluster_info["autoscale"]["min_workers"],
                                      'Current Cluster Max Workers':cluster_info["autoscale"]["max_workers"]
                                      }
            log_to_mlflow(params=current_cluster_config)
            

            if args.update_cluster:
                # Update cluster configuration
                print('User requested to edit cluster compute configurations...')
                cluster_info["spark_version"] = config["spark_version"] # change spark version as of now

                # Send PATCH request to update cluster configuration
                edit_cluster_url = f"{databricks_instance}/api/2.0/clusters/edit"
                response = requests.post(edit_cluster_url, headers=headers, json=cluster_info)

                # Check if the request was successful
                if response.status_code == 200:
                    print("\nCluster configuration update initiated. Waiting for changes, make take a few minutes...!!")
                    time.sleep(int(config['cluster_config_update_wait_time']))

                    # Send GET request to the cluster info endpoint after waiting period
                    response = requests.get(cluster_info_url, headers=headers)

                    # Check if the request was successful
                    if response.status_code == 200:
                        updated_cluster_info = response.json()

                        # Compare the initial and updated cluster configurations
                        if updated_cluster_info["spark_version"] == cluster_info["spark_version"]:
                            print("Cluster configuration updated successfully!")
                            print("Updated Cluster Spark:", updated_cluster_info["spark_version"])
                        else:
                            print("Cluster configuration not updated.")
                    else:
                        print("Failed to retrieve updated cluster information. Status code:", response.status_code)
                else:
                    print("Failed to initiate cluster configuration update. Status code:", response.status_code)

        else:
            print("Failed to retrieve cluster information. Status code:", response.status_code)
    else:
        print("No clusters found.")
