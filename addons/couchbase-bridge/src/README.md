# Apache Atlas Couchbase bridge
This bridge connects to a Couchbase cluster using DCP protocol 
and performs real-time analysis and metadata extraction from stored on the cluster documents.
The extracted metadata is then sent to Atlas via its REST API

## Configuration
The bridge uses environment variables for configuration.

### Atlas REST API
| Environment variable | Description                                                                                                                       | Default Value            |
|----------------------|-----------------------------------------------------------------------------------------------------------------------------------|--------------------------|
| ATLAS_URL            | Atlas REST API url                                                                                                                | "http://localhost:21000" |
| ATLAS_USERNAME       | Atlas REST API username                                                                                                           | "admin"                  |
| ATLAS_PASSWORD       | Atlas REST API password                                                                                                           | "admin"                  |

### Couchbase DCP connection
| Environment variable | Description                                                                                                                       | Default Value            |
|----------------------|-----------------------------------------------------------------------------------------------------------------------------------|--------------------------|
| CB_CLUSTER           | Couchbase Cluster connection string                                                                                               | "couchbase://localhost"  |
| CB_USERNAME          | Couchbase Cluster username                                                                                                        | "Administrator"          |
| CB_PASSWORD          | Couchbase Cluster password                                                                                                        | "password"               |
| CB_ENABLE_TLS        | Use TLS                                                                                                                           | false                    |
| CB_BUCKET            | Couchbase bucket to monitor                                                                                                       | "default"                |
| CB_COLLECTIONS       | Comma-separated list of collections to monitor with each collection listed as <scope>.<collection>                                |                          |
| DCP_PORT             | DCP port to use                                                                                                                   | 11210                    |
| DCP_FIELD_THRESHOLD  | A threshold that indicates in what percentage of analyzed messages per collection  a field must appear before it is sent to Atlas | 0                        |
| DCP_SAMPLE_RATIO     | Percentage of DCP messages to be analyzed in form of a short between 0 and 1.                                                     | 1                        |
