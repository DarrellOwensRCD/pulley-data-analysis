''' 
This program prepares regression data by calculating the duration of time
to get a permit approval + the number of repeated reviews and the worst offender
repeat review catagories. Goal is to use the data in R to run a Regression on whether
reviews. This was ultimately used in the presentation to determine if high repeated reviews
were associated with certain reviews. 
'''
import csv
import json
import re
import statistics
from datetime import datetime
from google.cloud import bigquery

# Helper method that accepts a dictionary of reviews-types where repetition occurred
# and return the highest repeated review, how many times it repeats and the number of
# repeated reviews overall
def count_repeats(dic):
    repeats = 0 # Overall count of repeat reviews
    for key in dic.keys():
        if dic[key] > 1: # Checks if review in dictionary queue had repeats
            repeats += 1
    return repeats, max(dic, key=lambda k: dic[k]), max(dic.values())


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
work = {}
for row in results:
    # Row values can be accessed by field name or index.
    x = eval(row[0]) # BigQuery data results
    num_reviews = 0 # Overall count of reviews
    repeats = {}    # Dictionary key: Name of Review; Dic Val: Count of Repeats
    permit = x['FOLDER DETAILS'][0]['Permit/Case'] # Unique Permit ID
    work_type = x['FOLDER DETAILS'][0]['Work Type'] # Type of Project Perit uses
    filed = x['FOLDER DETAILS'][0]['Application Date']
    filed = datetime.strptime(filed, "%b %d, %Y")
    apprv = x['FOLDER DETAILS'][0]['Issued']
    apprv = datetime.strptime(apprv, "%b %d, %Y") # Approval Time of Permit
    # Divide up work into residential or commericial
    biz = ("Residential " if x['FOLDER DETAILS'][0]
           ['Sub Type'][0] == 'R' else "Commericial ")
    ''' 
    Loop Description: Count the number of repeated reviews for given permit
    Within loop: 
        if a review was conducted at all i.e. attempts >= 1
            Iterate review count
            Acquire name_type of the review
            if the review is not a disqualified process Jordan and I agreed are not real reviews:
                if the review type is not in the repeat's dictionary
                    add it 
                if the review had > 1 attempts:
                    set that to the review's dictionary value
                if the review has 1 attempt:
                    increment +1 in the value of the review's key
    '''
    for rev in x['PROCESSES AND NOTES']:
        if int(rev['# of Attempts']) > 0:
            num_reviews += 1
            p = rev['Process Description']
            if p != 'Web Application Acceptance' and p != 'Plan Review Administration' and p != 'Revisions After Issuance' and p != 'Coordinating Reviews':
                if rev['Process Description'] not in repeats:
                    repeats[rev['Process Description']] = 0
                # Sometimes they mark down the # of repeats, sometimes they just
                # say attempt #1 for multiple identical entries
                if(int(rev['# of Attempts']) > repeats[rev['Process Description']]):
                    repeats[rev['Process Description']] = int(
                        rev['# of Attempts'])
                elif (int(rev['# of Attempts']) == 1):
                    repeats[rev['Process Description']] += 1
    # With the dictionary of deliquent reviews assembled, acquire 
    # stats on the worst offender, it's record and overall repeated reviews for permit
    if len(repeats) > 0:
        repeat_counts, wr, wrk = count_repeats(repeats)
    else:
        repeat_counts, wr, wrk = 0, "None", 0
    # Save this data in a dictionary for that permit ID along with general permit info
    # including time to approve, work type and whether its residential or commericial
    work[permit] = [(apprv - filed).days, biz, work_type, num_reviews, repeat_counts, wr, wrk]
with open("../results/duration_for_every_permit.csv", 'w', newline='\n') as fp:
    w = csv.writer(fp, delimiter=',')
    w.writerows([["Permit Name", "Res / Com", "Duration", "Work Type", "Count of Reviews", "Occurances of Repeat Reviews", "Worst Review", "WR Count"]])
    for key in work.keys():
        w.writerows([[key, work[key][1], work[key][0], work[key][2], work[key][3], work[key][4], work[key][5], work[key][6]]])
    fp.close()
