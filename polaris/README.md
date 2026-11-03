# Lab: Deploy Polaris using a Helm chart (Kubernetes)

This lab deploys Apache Polaris on the Kubernetes namespace of our Onyxia account. Polaris is an Iceberg REST catalog: it keeps the current state of the Iceberg tables and it checks who can read or change them. This deployment is needed for the next lab, where Spark will use Polaris as its Iceberg catalog.

## Architecture

This section gives a short overview of what we deployed.

- **PostgreSQL** (`polaris-postgresql-0`): the database where Polaris stores its catalogs, principals, roles and table pointers.
- **Polaris server** (`polaris-...`): the REST catalog API on port 8181 (`http://polaris:8181`) and the management/health API on port 8182 (`http://polaris-mgmt:8182`). Before it starts, an init container (`polaris-admin-tool`) waits for PostgreSQL and creates the realm `POLARIS` with the `root` principal.
- **Bootstrap Job** (`polaris-bootstrap`): a Helm post-install hook. It calls the Polaris REST API to create the users, the roles, the catalog and the privileges.

Polaris uses role-based access control (RBAC). Privileges are not given directly to users, but through a chain of roles:

| Principal (user) | Principal role | Catalog role | Main privileges |
|---|---|---|---|
| `dev` | `data_engineer` | `catalog_contributor` | `CATALOG_MANAGE_ACCESS`, `CATALOG_MANAGE_METADATA`, `CATALOG_MANAGE_CONTENT` |
| `analyst` | `data_analyst` | `catalog_reader` | `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES`, `CATALOG_READ_PROPERTIES` |

So `dev` can create and change tables, and `analyst` can only read them.

## Prerequisites: reduce the resources of the VS Code service

This section explains why we had to change the resources of our Onyxia service first.

Our namespace has a resource quota. Before the lab, the `vscode-pyspark` service used all the CPU and memory limits, so there was no space for Polaris:

| Resource | Used before | Hard limit |
|---|---|---|
| `limits.cpu` | 4 | 4 |
| `limits.memory` | 4000Mi | 4Gi (4096Mi) |

We patched the StatefulSet of the service to use `cpu: 2000m` and `memory: 2500Mi` as limits (and `50m` / `200Mi` as requests). The pod restarted, and `cat /sys/fs/cgroup/memory.max` showed `2621440000` (2500Mi). After the patch, the quota showed `limits.cpu 2/4` and `limits.memory 2500Mi/4Gi`.

With the patch, there is enough space for the new pods:

| Pod | CPU limit | Memory limit |
|---|---|---|
| VS Code service | 2 | 2500Mi |
| PostgreSQL | 0.25 | 256Mi |
| Polaris (with init container) | 0.5 | 512Mi |
| Bootstrap Job | 0.2 | 56Mi |
| **Total** | **2.95 / 4** | **3324Mi / 4096Mi** |

Note: the pod restart deleted the AWS profile in `~/.aws`, but `~/work` was kept because it is a persistent volume.

## Deployment files

This section describes the files used by Helm.

The files were created with the script `polaris/create-files.sh`:

- `polaris/deployment/Chart.yaml`: our chart, with two dependencies: the Polaris chart (1.8.0) and the Bitnami PostgreSQL chart (18.12.15).
- `polaris/deployment/values.yaml`: our settings: PostgreSQL user, database and resources, JDBC persistence for Polaris, internal authentication, resources, environment variables from the ConfigMap and the Secrets, and the init container that bootstraps the realm.
- `polaris/deployment/templates/bootstrap-job.yaml`: the Job that creates the principals, roles, catalog and grants with `curl` and `jq`.
- `polaris/deployment/templates/polaris-db-secret.yaml`: the Secret with the database user and JDBC URL. The password is generated automatically by the secret generator.

## ConfigMap and Secrets

This section explains the configuration objects that we created before the installation.

The pods read their configuration from Kubernetes objects, so these objects must exist before `helm install`:

| Object | Type | Keys | Content |
|---|---|---|---|
| `polaris-config` | ConfigMap | 7 | catalog name, Polaris URIs, realm, client ID, base location, S3 endpoint |
| `polaris-credential` | Secret | 1 | password of the `root` principal |
| `polaris-s3` | Secret | 3 | AWS credentials, so Polaris can access the S3 bucket |

The catalog name is our namespace (`user-k-sison-ece`) and its base location is `s3://user-k-sison-ece/warehouse/`.

Note: the `polaris-s3` Secret contains our temporary STS credentials from Onyxia. When they expire, Polaris cannot access S3 anymore, and we must create this Secret again with new credentials.

## Deployment with Helm

This section shows how we installed Polaris.

From the `polaris/` folder, we downloaded the two dependency charts with `helm dependency update ./deployment`, then we installed the release:

```bash
helm upgrade --install polaris ./deployment --wait --timeout 10m
```

We added `--timeout 10m` because the default timeout (5 minutes) can be too short when the images are downloaded for the first time. The result was `STATUS: deployed` (revision 1). The pods were:

| Pod | Status |
|---|---|
| `polaris-6cf856d5b7-9sw4d` | Running (1/1) |
| `polaris-postgresql-0` | Running (1/1) |
| `polaris-bootstrap-xjb68` | Completed |

The health check `curl -s http://polaris-mgmt:8182/q/health` returned `"status": "UP"`, with `Database connections health check: UP`. This means that Polaris is connected to PostgreSQL.

## Credentials of the principals

This section explains how we saved the credentials of `dev` and `analyst`.

The bootstrap Job prints the credentials (`clientId` and `clientSecret`) of each new principal only once, in its logs. We extracted them with `kubectl logs job/polaris-bootstrap`, `grep` and `jq` into the file `/home/onyxia/.polaris-credentials.env`, as `POLARIS_DEV_CREDENTIAL` and `POLARIS_ANALYST_CREDENTIAL` (format `clientId:clientSecret`).

These credentials are OAuth2 client credentials: Spark will use them to get an access token from Polaris. They are secrets, so they are not in this repository. The file is outside `~/work`, so it will be deleted when the pod restarts. We copied the credentials to our own computer.

## Checks with the REST API (Advanced)

This section checks the objects created by the bootstrap Job.

We first got an access token for `root` from `/api/catalog/v1/oauth/tokens` (`grant_type=client_credentials`). The token is a JWT signed by Polaris (it starts with `eyJhbGciOiJSUzI...`, which means `{"alg":"RS256"...`). Then we used the management API (`/api/management/v1`) with this token:

| Request | Result |
|---|---|
| `GET /catalogs` | `user-k-sison-ece`, type `INTERNAL`, `default-base-location = s3://user-k-sison-ece/warehouse/` |
| `GET /principals` | `root`, `dev`, `analyst` |
| `GET /principal-roles` | `service_admin`, `data_engineer`, `data_analyst` |
| `GET /catalogs/user-k-sison-ece/catalog-roles` | `catalog_admin`, `catalog_contributor`, `catalog_reader` |

The example output of the lab shows `data_scientist`, but the bootstrap Job creates `data_analyst`, so our result is correct.

Note: the Polaris catalog uses the same `warehouse/` folder as the Hadoop catalog of the Iceberg lab, but it is a different catalog. Polaris does not know the old tables `silver/orders` and `silver/users`: its catalog is empty for now.

## Problems met during the lab

This section lists the problems we had and how we solved them.

- **Long code in the terminal**: the 600-line block that creates the files broke the terminal when we pasted it, and it also ran `set -e` in the interactive shell. We closed the terminal, pasted the block into a script file in the VS Code editor (`polaris/create-files.sh`), and ran it with `bash polaris/create-files.sh`.
- **Folder for Helm**: the files are created in `polaris/deployment/`, but the Helm commands use `./deployment`, so they must be run from the `polaris/` folder.
- **`kubectl get pods -w` never ends**: this is normal, it is a watch. We stopped it with `Ctrl+C`.
- **Pod restart**: the patch of the StatefulSet restarted the VS Code pod, so we had to configure the AWS profile again.

## Teardown

This section explains how to remove Polaris. We did not run it, because Polaris is needed for the next lab.

```bash
helm uninstall polaris
kubectl delete job polaris-bootstrap
kubectl delete secret polaris-s3 polaris-credential
kubectl delete configmap polaris-config
kubectl delete pvc data-polaris-postgresql-0
```