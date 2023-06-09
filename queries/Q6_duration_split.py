''' 
This is simply the average version of the Q6 median duration split .py program
which contains line by line breakdowns of the algorithm. This program is 
identical to the median version except workAgg values are summation values and not a list.
'''
# Number of Apps by Project Type
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
all = 0
all_minus = 0  # Addition, Shell and Relocation Removed
count = 0
count_minus = 0
for row in results:
    # Row values can be accessed by field name or index.
    x = eval(row[0])
    permit = x['FOLDER DETAILS'][0]['Sub Type']
    work = x['FOLDER DETAILS'][0]['Work Type']
    filed = x['FOLDER DETAILS'][0]['Application Date']
    filed = datetime.strptime(filed, "%b %d, %Y")
    apprv = x['FOLDER DETAILS'][0]['Issued']
    apprv = datetime.strptime(apprv, "%b %d, %Y")
    # Divide up work into residential or commericial
    work = ("Residential " if permit[0] == 'R' else "Commericial ") + work
    # Put duration data in a dictionary of each permit using the worktype as a key
    if work in workTypes:
        workTypes[work] += 1
        workAgg[work] += apprv - filed
    else:
        workTypes[work] = 1
        workAgg[work] = apprv - filed
    count += 1
    # Now we calculate overall average aggregating all work type permits
    if all == 0:
        all = apprv - filed
        if 'Shell' not in work and 'Relocation' not in work and (
                'Addition' not in work or ('Addition' in work and 'Remodel' in work)):
            all_minus = apprv - filed
            count_minus += 1
    else:
        all += apprv - filed
        if 'Shell' not in work and 'Relocation' not in work and (
                'Addition' not in work or ('Addition' in work and 'Remodel' in work)):
            all_minus += apprv - filed
            count_minus += 1
with open("../results/Q6work_duration_average_split_R_C.csv", 'w', newline='\n') as fp:
    w = csv.writer(fp, delimiter=',')
    w.writerows([["Work Type", "Average Days Approved", "Number of Projects"]])
    w.writerows([["ALL WORK", round(all.days / count, 1), count]])
    w.writerows([["ALL WORK with relocation, addition and shell removed", round(
        all_minus.days / count_minus, 1), count_minus]])
    for key in workTypes.keys():
        w.writerows([[key, round(workAgg[key].days / workTypes[key], 1), workTypes[key]]])
    fp.close()
