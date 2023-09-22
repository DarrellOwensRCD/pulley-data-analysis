
import csv
import json
import statistics
from datetime import datetime
from google.cloud import bigquery
def maxed(reviews ):

# Instantiate a client
client = bigquery.Client.from_service_account_json('credentials/key.json')

# Define your query
query = """
SELECT
	`data`
FROM
  `pulley-3da02.tracking_prod.approvals`
WHERE
	(applicationDate BETWEEN '2020-01-01' AND '2023-09-01') AND (jurisdictionId = 'denver-city-co') AND JSON_VALUE(data, '$.FOLDER DETAILS[0]."Issued"') is not NULL;
"""

# Run the query
query_job = client.query(query)

# Fetch the results
results = query_job.result()
reviews = {}  # Number of permits a specific review applies to, by review type
# Iterate through each permit and increment which review went through the
# most re-attempts for one permit
for row in results:
    x = eval(row[0])
    seen = {}  # Key : Review; Value = count
    for rev in x['PROCESSES AND NOTES']:
        p = rev['Process Description']  # Store the process / review
        if rev['Start Date'] and rev['End Date'] and p != 'Web Application Acceptance' and p != 'Plan Review Administration' and p != 'Revisions After Issuance' and p != 'Coordinating Reviews':
		if p not in seen:
			seen[p] = 1
		else:
			seen[p] += 1
     # What review had the most repeats?
     record = max(seen,key = seen.get)
     if record not in reviews:
	# Review Type : [ Num of Record Highs, [ Collection of Record Reviews], Median of Record Reviews]
	reviews[record] = [1, [seen[record] ], seen[record]]
     else:
	reviews[record][0] += 1
	reviews[record][1].append(seen[record])
	reviews[record][2] = statistics.median(reviews[record][1])
with open("results/Q8most_cumbersome_reviews.csv", 'w', newline='\n') as fp:
    w = csv.writer(fp, delimiter=',')
    w.writerows([["","Amount of Review-Types Grouped"]])
    for key in permits.keys():
        w.writerows([[key_dict[key][0], round(statistics.median(
            total_duration[key]), 1), permits[key], key_dict[key][1]]])
    fp.close()
