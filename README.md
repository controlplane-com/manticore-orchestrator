# Control Plane Manticore Orchestrator
This project makes it dead simple to run and operate [Manticore Search](https://manticoresearch.com/) on [Control Plane](controlplane.com).

> [!IMPORTANT]
 This project assumes you are running Manticore Search on [Control Plane](controlplane.com). It will not work with any other hosting solution.

## Features
### Clustering Without the Hassle ###
The Orchestrator makes intelligent decisions about cluster formation based on the state of all nodes.
### Non-Invasive Architecture ###
The Orchestrator is designed to work with the community Manticore docker images. It operates Manticore much like a human would, primarily through the MySql interface.
### Zero-Downtime Data Imports ###
The Orchestrator uses Manticore's `indexer` tool to quickly ingest data from .tsv or .csv files. It uses a blue/green import pattern which has many beneficial properties:
1. While an import runs, the old data remains indexed and accessible. This is key for large imports which often take more than an hour.
2. Once the import has finished, the Orchestrator transitions between old and new data instantaneously, across all running nodes.
3. To save space, the old data is deleted in the background.
### First-Class Support For the Idiomatic Main+Delta Pattern ###
Define a table schema using JSON, and instantly get a distributed table that:
1. Has a "main" child table, intended to be changed only by the import process (although that's up to you).
2. Has a "delta" child table. The delta table is always part of the Manticore cluster, leveraging Manticore's native replication capability.
3. Load balances queries between all healthy nodes in the cluster
4. Optionally, includes the main table in the cluster. This is not recommended, but helps in cases where you need to make changes to the main table between imports.
### A Helpful UI ###
<img width="1908" height="774" alt="image" src="https://github.com/user-attachments/assets/dc2a5514-5d4e-4ec2-a253-d2a72b70ac13" />
Here you can
- View the health of the cluster
- See the status of the tables managed by the Orchestrator
- Execute queries against your cluster, either to a specific node, or broadcast to all nodes at once.
- Start an import and track its progress.
- Repair the cluster in the event of a split brain with a single click.
- View import and repair operation history

## Supported Manticore Versions ##
The orchestrator has been tested with version `15.1.0`

## Ready To Deploy? ##
There is a helm template available [here](https://github.com/controlplane-com/templates/tree/main/manticore) which can be used to deploy this project to Control Plane. [Click here](https://docs.controlplane.com/guides/cpln-helm#manage-helm-releases) to learn about managing helm releases on Control Plane. 
