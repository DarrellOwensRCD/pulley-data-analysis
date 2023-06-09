''' Goal is to calculate the estimated median review duration in days provided via 
the schedule estimates from each review. Reviews are also grouped into topics
so grouping size is provided. '''
import csv
import json
import statistics
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
total_duration = {}  # Sum up number of days it takes to review
# Example. Envior Review occurred 20 times in the dataset
permits = {}  # Number of permits a specific review applies to, by review type
# Example. Envior Review: 10 permits had it
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
key_dict = {  # Number of permits per process(s) used for printing in the CSV
    'group1': ['Historic Reviews', len(group1)],
    'group2': ['Residential Reviews', len(group2)],
    'group3': ['Commericial Reviews', len(group3)],
    'group4': ['Utilities and Waste Reviews', len(group4)],
    'group5': ['Administrative & Planning Reviews', len(group5)],
    'group6': ['Enviormental Reviews', len(group6)],
    'group7': ['Safety Reviews', len(group7)]
}
# Iterate through each permits
for row in results:
    x = eval(row[0])
    queue = []  # Store currently visited reviews
    # For each review / process per permit find which grouping it belongs to
    for rev in x['PROCESSES AND NOTES']:
        p = rev['Process Description']  # Get the process / review
        if rev['Start Date'] and rev['Scheduled End Date'] and p != 'Web Application Acceptance' and p != 'Plan Review Administration' and p != 'Revisions After Issuance' and p != 'Coordinating Reviews':
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
                # Never want to be here
                print(p)
                print("ERROR")
            # Now, calculate a durational time in days that specific review is projected to take
            # and put the time in a dictionary of times for the associated group named 'total_duration'
            duration = datetime.strptime(rev['Scheduled End Date'], "%b %d, %Y") - datetime.strptime(rev['Start Date'], "%b %d, %Y")
            if group not in total_duration:
                total_duration[group] = [duration.days]
            else:
                total_duration[group].append(duration.days)
	    # Use a queue system to determine how many times an instance of these grouped
        # reviews has occured. 
	    if group not in queue:
                queue.append(group)
                if group not in permits:
                    # Num of permits this specific review applies to
                    permits[group] = 1
                else:
                    permits[group] += 1
with open("results/Q7review_duration_median_scheduled.csv", 'w', newline='\n') as fp:
    w = csv.writer(fp, delimiter=',')
    w.writerows([["Review Type ", "Median Number of Reviews Per Permit in Days", "Number of Permits That underwent review", "Amount of Review-Types Grouped"]])
    for key in permits.keys():
        w.writerows([[key_dict[key][0], round(statistics.median(total_duration[key]), 1), permits[key], key_dict[key][1]]])
    fp.close()
