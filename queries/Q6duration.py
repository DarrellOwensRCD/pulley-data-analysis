'''Calculates the average number of days a permit takes to be approved by work type
and by permit-type'''
import csv
import json
import re
from datetime import datetime
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
results = query_job.result()
# Number of permits / works by types
permitTypes = {}
workTypes = {}
# Aggregate of durational times to be divided per permit / work
permitAgg = {}
workAgg = {}
all = False
count = 0

''' Iterates through each permit, calculates length of time it takes to approve 
and appends that value to an aggregate in a dictionary. Then does the same for work-type
data and appends to aggregate in dictionary. '''

for row in results:
    x = eval(row[0])
    permit = x['FOLDER DETAILS'][0]['Sub Type']
    work = x['FOLDER DETAILS'][0]['Work Type']
    filed = x['FOLDER DETAILS'][0]['Application Date']
    filed = datetime.strptime(filed, "%b %d, %Y")
    apprv = x['FOLDER DETAILS'][0]['Issued']
    apprv = datetime.strptime(apprv, "%b %d, %Y")
    # Permit Type First
    if permit in permitTypes:
        permitTypes[permit] += 1
        permitAgg[permit] += apprv - filed
    else:
        permitTypes[permit] = 1
        permitAgg[permit] = apprv - filed
    # Work Type Second
    if work in workTypes:
        workTypes[work] += 1
        workAgg[work] += apprv - filed
    else:
        workTypes[work] = 1
        workAgg[work] = apprv - filed
    if not all:
        all = apprv - filed
    else:
        all += apprv - filed
    count += 1
# With aggregates assembled, write back average values by dividing counts for all permits, 
# perits by type and permits by work-type
with open("results/Q5permit_duration.csv", 'w', newline='\n') as fp:
    w = csv.writer(fp, delimiter=',')
    w.writerows([["Permit Type", "Average Days Approved", "Number of Projects"]])
    w.writerows([["ALL PERMITS", round(all.days / count,1), count]])
    for key in permitTypes.keys():
        w.writerows([[key, round(permitAgg[key].days /
                     permitTypes[key],1), permitTypes[key]]])
    fp.close()
with open("results/Q5work_duration.csv", 'w', newline='\n') as fp:
    w = csv.writer(fp, delimiter=',')
    w.writerows([["Work Type", "Average Days Approved", "Number of Projects"]])
    w.writerows([["ALL WORK", round(all.days / count,1), count]])
    for key in workTypes.keys():
        w.writerows([[key, round(workAgg[key].days / workTypes[key], 1), workTypes[key]]])
    fp.close()
