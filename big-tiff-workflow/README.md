*** Under Construction - Not ready for use ***

# Big Tiff Workflow

A simple workflow to process a large area using a Tiled approach and then stiching the result together to produce a Big Tiff.
The final Big Tiff step can have a different specification to the other argo worker Pods to ensure it can load all the tiled data. Zarr is used an interim format for the Tile processing outputs.

## Processing steps

1. Break region of interest up into tiles
2. Launch dask cluster
3. Process Tiles and save to Zarr
4. Merge result in a big Pod

### 1. Break region of interest up into tiles
* `tile_ds_generator.py`
* Determines what datasets are in the tiles in the region of interest
* Divide the tiles into batches for each cluster to process

### 2. Launch dask cluster, Perform Predictions
* `tile_process.py`
* Parallel stage for all batches
* Launches X  # of Dask clusters
* Limited by configuration on AWS account (~5K cores)
  * Must account for others using the cluster in the account
* Will reuse nodes in cluster as necessary

### 3. Merge Result
* Save TIF in `save_tiff.py`

### 4. Exit Handler
* `exit_handler.py`
* Cleanup

## Argo Process Workflow

### Set up EASI to be able to run the workflow

#### Setup a Service Account

Big Tiff workflow requires service account, which allows for the configuration of:
  1. Set of authorizations for IAM role, for access to S3 bucket
  2. Ability to run Argo workflows via RBAC Manager, allows for creation of K8S pods, etc.(configured in Flux)
  3. Service account token for accessing Dask Gateway (created in Terraform and registered in JupyterHub to allow a JH login, i.e. Argo login is analagous to a user JH login to Dask Gateway)
* The service account allows Argo to shutdown entire cluster if desired, which may be needed for error conditions; _clusters follow service accounts_
* The service account and associated secrets are created in Terraform:
  * easi > **argo-project.tf**:  secrets
  * easi > **sa-argo-project.tf**: service account, including IAM role config
* To see details for the service account, run the following in the **easihub** namespace:
```
$ kubectl get sa
$ kubectl describe sa argo-project
```
> You will notice the annotation for the IAM role
* Prepare required credentials for Argo to access ADO repository
  * Create a user with read-only access to the ADO repository (noted above)
  * These accounts must be associated with an actual user, so it may be necessary to create a user account named "service-ArgoUser" in ADO. This is because ADO has no concept of service accounts for access tokens to Git.
  * Use a K8S 'Kustomize Generator' to generate the credentials
    * `kustomization.yaml` path=/utilities/azure-devops-project-git-ro-creds/kustomization.yaml) references a credentials file named **git-credentials** that contains creds of the format:
      https://AzDO-Org\<username\>:\<password\/personal_access_token\>@dev.azure.com
    * personal access token in user's environment (i.e. Rob's) is named the same as the **secretGenerator** in the kustomization.yaml file, e.g. **azure-devops-project-git-ro-creds**, which has read-only access to the Git repositories
    *  run this command in the same directory to create the credentials in the system: <br/>`kubectl --namespace easihub apply -k .`<br/>
      * Base64-encodes the credentials
      * adds them to the Configuration > Secrets module of the K8S cluster, named **azure-devops-project-git-ro-creds-\<HASH\>**
  * **TODO**: currently Argo workflows in Prod are associated with Rob's ADO account -- create a new account specifically for Argo workflows

#### Upload Model Files to S3

* Use the notebook UpLoadFiles.ipynb] to manually upload the files to S3

