'''Calculates the median number of days a permit takes to be approved by work type
and divided up by residential and commericial'''
import csv
import json
import re
import statistics
from datetime import datetime
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
all = []
all_minus = []
count = 0
count_minus = 0
for row in results:
    x = eval(row[0])
    permit = x['FOLDER DETAILS'][0]['Sub Type'] # Name of permit 
    work = x['FOLDER DETAILS'][0]['Work Type'] # Name of work type
    filed = x['FOLDER DETAILS'][0]['Application Date']
    filed = datetime.strptime(filed, "%b %d, %Y")
    apprv = x['FOLDER DETAILS'][0]['Issued']
    apprv = datetime.strptime(apprv, "%b %d, %Y")
    # Divide up work into residential or commericial and append R/C to worktype stirng
    work = ("Residential " if permit[0] == 'R' else "Commericial ") + work
    # Inserting the duration of time to approval for this permit into the queue with work_type as label
    # This list within a dictionary of work types will be used to calculate median
    if work in workTypes:
        workTypes[work].append((apprv - filed).days)
    else:
        workTypes[work] = [(apprv - filed).days]
    all.append((apprv - filed).days) # This assembles all permits for overall durational info
    # Assembles duration for all permits minus Shell, Relocation, and Addition (but not Addition & Remodel)
    if 'Shell' not in work and 'Relocation' not in work and ('Addition' not in work or ('Addition' in work and 'Remodel' in work)):
        all_minus.append((apprv - filed).days)
        count_minus += 1
    count += 1
# Write Back with Calculations for the Median
with open("../results/Q6work_duration_median_split_R_C.csv", 'w', newline='\n') as fp:
    w = csv.writer(fp, delimiter=',')
    w.writerows([["Work Type", "Median Days Approved", "Number of Projects"]])
    w.writerows([["ALL WORK", round(statistics.median(all), 1), count]])
    w.writerows([["ALL WORK with relocation, addition and shell removed", round(
        statistics.median(all_minus), 1), count_minus]])
    for key in workTypes.keys():
        w.writerows([[key, round(statistics.median(workTypes[key]), 1), len(workTypes[key])]])
    fp.close()
