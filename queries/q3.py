# Number of Applications by Month and Year
import csv
from google.cloud import bigquery

# Instantiate a client
client = bigquery.Client.from_service_account_json('credentials/key.json')

# Define your query
query = """
SELECT
  EXTRACT(YEAR FROM applicationDate) AS year,
  EXTRACT(MONTH FROM applicationDate) AS month,
	EXTRACT(DAY FROM applicationDate) as day,
  COUNT(*) AS permits
FROM
  `pulley-3da02.tracking_prod.approvals`
WHERE
	(applicationDate BETWEEN '2022-08-01' AND '2023-03-01') AND (permitType = 'Plan Review')
GROUP BY
  year, month, day
ORDER BY
  year, month, day;
"""

# Run the query
query_job = client.query(query)

# Fetch the results and write to CSV
fp = open("results/Q3_YearMonthResCom.csv", "w")
query_job.result().to_dataframe().to_csv(fp, index=False)
