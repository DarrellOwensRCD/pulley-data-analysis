'''
Acquires a raw count of permits and their ratio of overall permit.
'''
import csv
import json
from google.cloud import bigquery

# Instantiate a client
client = bigquery.Client.from_service_account_json('../credentials/key.json')

# Define your query
query = """
SELECT
	`data`
FROM
  `pulley-3da02.tracking_prod.approvals`
WHERE
	(applicationDate BETWEEN '2022-08-01' AND '2023-03-01') AND (permitType = 'Plan Review');
"""

# Run the query
query_job = client.query(query)

# Fetch the results
results = query_job.result()
types = {}
count = 0
for row in results:
    x = eval(row[0])
    permit = x['FOLDER DETAILS'][0]['Sub Type']
    value = 1
    if permit in types:
        value += types[permit]
    types[permit] = value
    count += 1
# Write Back
with open("../results/Q4_permits_by_typeSubmitted.csv", 'w', newline='\n') as fp:
    w = csv.writer(fp, delimiter=',')
    w.writerows([["Permit Type", "Raw Count", "Ratio"]])
    for key in types.keys():
        w.writerows(
            [[key, types[key], str(round(types[key] / count * 100, 1)) + '%']])
    fp.close()
