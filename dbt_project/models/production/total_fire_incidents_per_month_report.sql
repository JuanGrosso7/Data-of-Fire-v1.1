SELECT
    TO_CHAR(DATE_TRUNC('month', incident_date), 'YYYY Month') AS "Month",
    COUNT(id) AS "Total Incidents"
FROM
    {{ ref('fire_incidents') }}
GROUP BY
    DATE_TRUNC('month', incident_date)
ORDER BY
    DATE_TRUNC('month', incident_date)


