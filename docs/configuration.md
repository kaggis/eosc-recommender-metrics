# Licence

<! --- SPDX-License-Identifier: CC-BY-4.0  -- >

# EOSC Recommender Metrics Framework

## Introduction

The system consists of three key components: a Preprocessor for fetching and storing data in MongoDB, an RSmetrics module for complex metric computations, and a user-friendly UI dashboard with a RESTful API for seamless report delivery and visual presentation of metrics.

## Configuration Overview

There are three main aspects that need to be configured so that it can work properly:

* the MongoDB Server,
* the Preprocessor and the RS Metrics, and
* the API/UI configuration.

## Configuration Files

Configuration files of each of the four necessary components are:
* `/etc/mongod.conf` - configuration of MongoDB Server
* `config.yaml` - configuration of Preprocessor and RS Metrics
* `.env` - configuration of API/UI

## Configuration Parameters

### Mongo DB Server
MongoDB's official documentation provides a detailed guide to configuring the MongoDB server. Find comprehensive insights into server optimization and behavior settings at [MongoDB Server Configuration](https://docs.mongodb.com/manual/reference/configuration-options). This resource is your go-to for fine-tuning performance and tailoring the MongoDB server to your specific requirements.

#### Example configuration
This example covers key aspects such as data storage, logging, network interfaces, security, and replication. Users should adjust the paths and settings based on their specific environment and requirements. It is advisable to always refer to the official MongoDB documentation for the most up-to-date and accurate information.
```yaml
# mongod.conf

# Where and how to store data.
storage:
  dbPath: /var/lib/mongodb
  journal:
    enabled: true

# Basic settings
systemLog:
  destination: file
  logAppend: true
  path: /var/log/mongodb/mongod.log

# Network interfaces
net:
  port: 27017
  bindIp: 127.0.0.1

# Security settings
security:
  authorization: enabled

# Replication settings
replication:
  replSetName: "rs0"
```

### Preprocessor and RS Metrics
Necessary information regarding the configuration of the Preprocessor and RS Metrics along with the configuration parameters can be found at the [config](https://github.com/ARGOeu/eosc-recommender-metrics/blob/master/config.yaml), which is necessary in order for the components to work.

#### Example configuration
This examples covers the configuring of the Preprocessor and the RS Metrics components. The initial section specifies the names of the RS engines to register for connection and information retrieval. The subsequent section outlines the Mongo DB database for storing respective data captured from the Preprocessor along with the metrics calculations produced by the RS Metrics. Following that, the configuration addresses the source location for retrieving items from the EOSC Marketplace, along with specifying the relationship between the RS engines and the types of the items that are captured.
```yaml
# Set the desired connector
# a list of:
# name/tag, type of database, hostname, port, database
providers:
   - name: marketplace_rs
     db: "mongodb://localhost:27017/recommender_dev"
   - name: athena
     db: "mongodb://localhost:27017/athena_dev"
   - name: online_engine
     db: ""

datastore: "mongodb://localhost:27017/rsmetrics"

service:
    # Use the EOSC-Marketplace webpage
    # to retrieve resources and associate the page_id and the service_id
    store: './page_map' # or null
    service_list_url: 'https://localhost/replace/with/remote/example'
    # if true it keeps only published, otherwise all
    # this has an effect in exporting when from is set to 'source' 
    # and also in metrics calculations where service is considered
    published: true
    category:
       athena: [service]
       marketplace_rs: [service]
       online_engine: [training, data_source]
```

### API/UI
The configuration of API/UI specifies the locations from which to retrieve essential information for visualizing reports and corresponding details on statistics, metrics, KPIs, and graphs, which can be found at [.env](https://github.com/ARGOeu/eosc-recommender-metrics/blob/master/webservice/.env), which is necessary in order for the components to work.

#### Example configuration
This example covers the configuration of API/UI that indicate where to read the necessary information from in order to visualize the reports and the respective information about statistics, metrics, KPIs and graphs. Users should adjust the values based on their specific applications and environments. Additionally, they should ensure that sensitive information, such as secret keys and database URIs, is kept confidential and not shared in public repositories.
```yaml
RSEVAL_MONGO_URI=mongodb://localhost:27017/rsmetrics
RSEVAL_METRIC_DESC_DIR=../metric_descriptions
RSEVAL_SUPERVISOR_RPC_SERVER=http://localhost:9001/RPC2
RSEVAL_STREAM_USER_ACTIONS_JOBNAME=stream-user-actions
RSEVAL_STREAM_RECOMMENDATIONS_JOBNAME=stream-recommendations
RSEVAL_STREAM_MP_DB_EVENTS_JOBNAME=stream-mp-db-events
```

## Environmental Variables

- All necessary enviromental variables are described in the configuration files.

## Configuration Sources

- The configuration file for the MongoDB Server is conveniently located in a standard path.
- The configuration file path for the Preprocessor and the RS Metrics is given via the command line, therefore it can be anywhere.
- Order of precedence leads to no configuration values conflict.

## Configuration Management

All the configuration management is done manually through these three files.

## Security Considerations

Most of the information stored in the configuration files is not sensitive, apart from hostnames and ports. 

## Troubleshooting

### MongoDB server
Identifying misconfigurations in a MongoDB server involves examining logs for errors, validating the configuration file syntax with `mongod --configTest`, and checking critical parameters such as storage, network settings, and security configurations. Verify authentication and authorization settings, confirm proper file permissions, and ensure firewall and port accessibility. Real-time monitoring tools like mongostat aid in detecting anomalies and optimizing performance based on these findings.

### Preprocessor and RS Metrics
Ensuring the proper configuration of these components entails reviewing logs for potential errors and checking the report output, which notifies if any issues have occurred during the metrics computation.

### API/UI
To guarantee the appropriate configuration of these components, one must examine logs for potential errors, review the report output for issue notifications, incorporate API return calls for a thorough assessment, and observe the UI reports of any error notification in metrics computations.

## References
- [EOSC Recommender Metrics Framework README](https://github.com/ARGOeu/eosc-recommender-metrics/tree/master#readme)
- [MongoDB Server Configuration](https://docs.mongodb.com/manual/reference/configuration-options)
- [config](https://github.com/ARGOeu/eosc-recommender-metrics/blob/master/config.yaml)
- [.env](https://github.com/ARGOeu/eosc-recommender-metrics/blob/master/webservice/.env)
