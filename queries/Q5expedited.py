# Number of Apps by Project Type
import csv
import json
import re
from google.cloud import bigquery

# Instantiate a client
client = bigquery.Client.from_service_account_json('credentials/key.json')

# Define your query
query = """
SELECT
	`data`
FROM
  `pulley-3da02.tracking_prod.approvals`
WHERE
	(applicationDate BETWEEN '2022-08-01' AND '2023-03-01') AND (permitType = 'Plan Review') AND JSON_VALUE(data, '$.FOLDER DETAILS[0]."Issued"') is not NULL;
"""

# Run the query
query_job = client.query(query)

# Fetch the results
fp = open("results/Q4_CommericialPermits.csv", "w")
results = query_job.result()
types = {}
expedited = 0
express = 0
other = 0
for row in results:
    # Row values can be accessed by field name or index.
    x = eval(row[0])
    permit = x['FOLDER DETAILS'][0]['Description']
    if re.search('Expedited', permit, re.IGNORECASE):
        expedited += 1
    elif re.search('Express', permit, re.IGNORECASE):
        express += 1
    else:
        other += 1
# Write Back

with open("results/expedited_count.csv", 'w', newline='\n') as fp:
    fp.write("Permit Approval Times\n")
    fp.write(
        "Expedited Time: %d\nExpress Permits: %d\nStandard Time: %d" %
        (expedited, express, other))
    fp.close()
