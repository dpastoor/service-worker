# service-worker

#### Outline

Pulls from Google BigQuery to populate our databases

Spin off child worker with node cluster
var cluster = require('cluster);

loop through all log data, processing and inserting into the DB
 * post to seperate log on each result to keep track of how far worker got through log data (in case of crash).


wait for last response from DB, (result of last insert)

* fire off other worker to query db for velocity results
* populate redis caches with velocity info


1) Log_worker inserts into




1) First pass:
* upsert all CommitEvent type events to Repos table

2) Second pass:
* 



weekly velocities
javascript
python
R
