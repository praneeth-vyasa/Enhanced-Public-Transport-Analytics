create database trafic_condition 

create table trafic_transport_data(
    bus_id int primary key,
    route_number varchar(50),
    timestamp DATETIME,
    location varchar(100),
    direction varchar(50),
    vehicle_count int,
    avg_speed float,
    peak_hour varchar(10),
    weather_condition varchar(50),
    visibility float,
    temperature float,
    humidity float,
    wind_speed float,
    accidents int,
    roadwork varchar(50),
    traffic_signal_status varchar(50),
    congestion_level varchar(50),
    area varchar(100)
);

BULK INSERT trafic_transport_data
FROM 'C:\Users\2022\Desktop\Praneeth\New project\Traffic_Monitoring_System_Dataset.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2
);

select *from trafic_transport_data

go

CREATE or ALTER PROCEDURE analyze_transport_data
AS
BEGIN
    SELECT 
        trafic_transport_data.bus_id,
        trafic_transport_data.route_number,
        trafic_transport_data.location,
        trafic_transport_data.direction,
        trafic_transport_data.timestamp,
        trafic_transport_data.vehicle_count,
        CASE 
            WHEN trafic_transport_data.vehicle_count > (SELECT AVG(vehicle_count) FROM trafic_transport_data) THEN 'Peak'
            ELSE 'Normal'
        END AS period_type
    FROM trafic_transport_data
    JOIN (
        -- Calculate average vehicle count for each bus, route, location, direction, and hour
        SELECT
            bus_id,
            route_number,
            location,
            direction,
            DATEPART(HOUR, timestamp) AS hour,
            AVG(vehicle_count) AS avg_vehicle_count
        FROM trafic_transport_data
        GROUP BY bus_id, route_number, location, direction, DATEPART(HOUR, timestamp)
    ) subquery
    ON trafic_transport_data.bus_id = subquery.bus_id 
       AND trafic_transport_data.route_number = subquery.route_number 
       AND trafic_transport_data.location = subquery.location 
       AND trafic_transport_data.direction = subquery.direction 
       AND DATEPART(HOUR, trafic_transport_data.timestamp) = subquery.hour
    ORDER BY trafic_transport_data.timestamp;

    SELECT 
        location,
        AVG(vehicle_count) AS avg_vehicle_count
    FROM trafic_transport_data
    GROUP BY location
    ORDER BY avg_vehicle_count DESC
    OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;
END;

EXEC analyze_transport_data;

go

--creating view for vehical count trend
CREATE OR ALTER VIEW vehicle_count_trends AS
SELECT 
    YEAR(timestamp) AS year,
    MONTH(timestamp) AS month,
    DAY(timestamp) AS day,
    DATEPART(HOUR, timestamp) AS hour,
    AVG(vehicle_count) AS avg_vehicle_count,
    CASE 
        WHEN AVG(vehicle_count) > LAG(AVG(vehicle_count)) OVER (ORDER BY MIN(timestamp)) THEN 'Increasing'
        WHEN AVG(vehicle_count) < LAG(AVG(vehicle_count)) OVER (ORDER BY MIN(timestamp)) THEN 'Decreasing'
        ELSE 'Stable'
    END AS trend_status
FROM trafic_transport_data
GROUP BY 
    YEAR(timestamp),
    MONTH(timestamp),
    DAY(timestamp),
    DATEPART(HOUR, timestamp);


select * from vehicle_count_trends;

go

-- creating view for vehicle count anomalies 
CREATE OR ALTER VIEW vehicle_count_anomalies AS
WITH AvgStdDev AS (
    SELECT 
        AVG(vehicle_count) AS avg_count,
        STDEV(vehicle_count) AS std_dev
    FROM trafic_transport_data
)
SELECT 
    DATEPART(YEAR, timestamp) AS year,
    DATEPART(MONTH, timestamp) AS month,
    DATEPART(DAY, timestamp) AS day,
    DATEPART(HOUR, timestamp) AS hour,
    vehicle_count,
    CASE 
        WHEN vehicle_count > (SELECT AVG(vehicle_count) FROM trafic_transport_data) + std_dev THEN 'High Anomaly'
        WHEN vehicle_count <(SELECT AVG(vehicle_count) FROM trafic_transport_data) -  std_dev THEN 'Low Anomaly'
        ELSE 'Normal'
    END AS anomaly_status
FROM trafic_transport_data
CROSS JOIN AvgStdDev;

select * from vehicle_count_anomalies;
go 

-- Create the view to encapsulate the main analysis query
CREATE OR ALTER VIEW transport_data_analysis_view AS
SELECT 
    trafic_transport_data.bus_id,
    trafic_transport_data.route_number,
    trafic_transport_data.location,
    trafic_transport_data.direction,
    trafic_transport_data.timestamp,
    trafic_transport_data.vehicle_count,
    AVG(tt.avg_vehicle_count) AS avg_vehicle_count,
    CASE 
        WHEN trafic_transport_data.vehicle_count > AVG(tt.avg_vehicle_count) THEN 'Peak'
        ELSE 'Normal'
    END AS period_type
FROM trafic_transport_data
CROSS JOIN (
    SELECT AVG(vehicle_count) AS avg_vehicle_count
    FROM trafic_transport_data
) AS tt
GROUP BY 
    trafic_transport_data.bus_id,
    trafic_transport_data.route_number,
    trafic_transport_data.location,
    trafic_transport_data.direction,
    trafic_transport_data.timestamp,
    trafic_transport_data.vehicle_count;

go 

select * from transport_data_analysis_view;
