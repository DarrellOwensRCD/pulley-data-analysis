# Average Number of Reviews a Permit endures by Grouped Review-Type
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
all_process = {}  # Counting every instance of a specific (key.type) review occurring
# Ex. Envior Review occurred 20 times in the dataset
permits = {} # Number of permits a specific review applies to, by review type
# Ex. Envior Review: 10 permits had it
group1 = [
    'Historic Preservation',
    'Historic Review',
    'Historic Refund']  # Historic
group2 = ['AE BSPA', 'Residential Zoning Review', 'Tech Master Review']  # Res
group3 = ['Commercial Site Plan Review', 'Commercial Zoning Review',
          'Commercial Building Plans']  # Com
group4 = [
    'Industrial Waste Review',
    'Historic Intake',
    'Grading and Drainage',
    'AWU OWRS review',
    'Med Gas',
    'AW Industrial Waste',
    'AW On Site Sewage Facility',
    'AW TAP Application Review',
    'AW UDS TAP Plan Review']  # Utilities
group5 = [
    'Building Plans - Energy',
    'Building Plans - Plumbing',
    'Building Plans - Mechanical',
    'Building Plans - Electrical',
    'ATD SIF Review',
    'Special Inspections Review'
]  # Admin
group6 = [
    'Tree Ordinance Review',
    'AE Green Building',
    'Erosion Hazard Zone',
    'Flood Plain Review']  # Envior
group7 = ['Health', 'Structural Review', 'Design Standards Building',
          'Special Inspections Review ', 'Fire Review', ]  # Safety / Health
key_dict = {  # Number of permits per process(s)
    'group1': ['Historic Reviews', len(group1)],
    'group2': ['Residential Reviews', len(group2)],
    'group3': ['Commericial Reviews', len(group3)],
    'group4': ['Utilities and Waste Reviews', len(group4)],
    'group5': ['Administrative & Planning Reviews', len(group5)],
    'group6': ['Enviormental Reviews', len(group6)],
    'group7': ['Safety Reviews', len(group7)]
}
# Iterate through each permit
for row in results:
    x = eval(row[0])
    # First Do All Permits
    queue = []  # Store currently visited reviews
    # For each review / process per permit
    for rev in x['PROCESSES AND NOTES']:
        p = rev['Process Description']  # Store the process / review
        # If the reviews's been conducted
        if rev['Start Date'] and p!= 'Web Application Acceptance' and p != 'Plan Review Administration' and p != 'Revisions After Issuance' and p != 'Coordinating Reviews':
            if p in group1:
                group = 'group1'
            elif p in group2:
                group = 'group2'
            elif p in group3:
                group = 'group3'
            elif p in group4:
                group = 'group4'
            elif p in group5:
                group = 'group5'
            elif p in group6:
                group = 'group6'
            elif p in group7:
                group = 'group7'
            else:
                print(p)
                print("ERROR")
            # Store a count of that specific review in the dictionary
            if group not in all_process:
                all_process[group] = 1 # Quantity of Nth-type review
            else:
                all_process[group] += 1
            # If per this permit, we have already seen said review before
            # do not increment the quantity of permits it applies to
            if group not in queue:
                queue.append(group)
                if group not in permits:
                    permits[group] = 1 # Num of permits this specific review applies to
                else:
                    permits[group] += 1
with open("results/Q7review_analysis.csv", 'w', newline='\n') as fp:
    w = csv.writer(fp, delimiter=',')
    w.writerows([["Review Type ", "Average Number of Reviews",
                  "Number of Permits That underwent review", "Amount of Review-Types Grouped"]])
    for key in permits.keys():
        w.writerows([[key_dict[key][0], round(all_process[key] /
                    permits[key], 1), permits[key], key_dict[key][1]]])
    fp.close()
