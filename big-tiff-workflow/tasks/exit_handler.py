import logging
from dask_gateway import Gateway
from dask_gateway.auth import JupyterHubAuth

def shutdown_all_clusters(address,api_token):
    auth = JupyterHubAuth(api_token=api_token)
    gateway = Gateway(address=address,  auth=auth)
    clusterRpts = gateway.list_clusters()
    logging.info(f"Shutting down these running clusters:\n {clusterRpts}")
    for cluster in clusterRpts:
        c = gateway.connect(cluster.name)
        c.shutdown()