# Backport for ABFS Driver - Install Hadoop ABFS driver on cluster versions prior to v3.1

The [ABFS driver](https://hadoop.apache.org/docs/stable/hadoop-azure/abfs.html) allows **Hadoop** clusters and those frameworks that utilize the **Hadoop FileSystem** to connect to their data that is stored in [Azure Data Lake Storage Gen2](https://azure.microsoft.com/services/storage/data-lake-storage) (ADLS). This driver has been committed into the Apache code repository since version 3.1. 

For customers that want to connect to ADLS from existing older clusters, we have backported the driver. It is available from this repository in pre-compiled binary form (Java 7 and upwards). There is also a shell script which may be used to patch each node on the cluster.

## Patch the cluster with ABFS driver
To patch the cluster, all nodes in the cluster must be patched. The script patch-cluster-node.sh provided with the repo https://github.com/Azure/abfs-backport helps patch a node with a hadoop-azure jar with ABFS driver. The same has been tested on the following versions of HDP clusters (HDP-2.5.3, HDP-2.6.0, HDP-2.6.1, HDP-2.6.2 & HDP-2.6.5).

### Note:
The script needs to be run with -a option on one of the nodes. This is to patch the jar in the HDFS filesystem. All other invocations patch the local filesystem of each node in the cluster.

### Prerequisites

The json processor tool **jq** is required for the script to run.
Please install the same from the following link: https://stedolan.github.io/jq/download/

### How the script works

The script works with the HDP-2.5.2 release of the abfs-backport repo (https://github.com/Azure/abfs-backport/releases/tag/HDP-2.5.2). It downloads the jar bundled with the release and patches the cluster node with the same.

The existing jar and tag.gz files will be backed up with a suffix .original_{version}.

{version} will be of the format: {yyyy}-{MM}-{dd}-{HH}-{mm}-{ss}. 
  
### Usage

Usage: 

    patch-cluster-node.sh [-a] [-u HDFS_USER] [-t TARGET_VERSION] [-p DIRECTORY_PREFIX] [-P HDFS_DIRECTORY_PREFIX] [-R] [-?]

Where:

`-a`  Update HDFS contents. This switch should only be specified for ONE node in a cluster patch.

`-u HDFS_USER`  Specifies the user name with superuser privileges on HDFS. Applicable only if the -a switch is specified. The default value is hdfs.

`-p DIRECTORY_PREFIX` Specifies a prefix that is specific to the Hadoop distro & version to search for files to patch. The default value is /

`-P HDFS_DIRECTORY_PREFIX`  Specifies a prefix that is specific to the Hadoop distro & version to search on HDFS for files to patch. The default value is /

`-R`  Rollback installation. Restores previously backed up versions of hadoop-azure jar file. Rollback for HDFS should follow same model as deployment. Specify the backup version for the rollback. Ex: Specify 2020-06-07-10-10-10 for the backup file named hadoop-azure.*.jar.original_2020-06-07-10-10-10
