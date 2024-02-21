---Driver Sanitisation on SSA Fleet Accounts----
----The goal is to clear up inactive drivers from our Fleet accounts----

---The Ask:
---Unassigned drivers that complete trips on : Unassigned drivers in the last 14 days that have taken  trip within that period of time.
--Assigned drivers that haven't completed trips on  in the Last 7 days (from the date the query was run) from today.

---Questions:

---Q1: Which products/market does this request apply to?
---R1: All SSA markets where we have products

---Q2: What time period is required for Unassigned drivers that complete trips on �?
---R2: From the time they were smart assigned and for the ones that were not smart assigned, we can look at the last 14 days

---Q3: Assigned drivers for what period of time?
---R3: Last 7 days from when the data was pulled. therefore, it should return drivers who haven�t completed any trip in over 7 days. 


---===============The Solution================------

----01: UNASSIGNED DRIVERS
--Unassigned Drivers before the last 14 days that are still working in the last 14 Days on any given day.--

-- 'active_drivers' CTE: Get list of currently active drivers
WITH active_drivers AS (
    SELECT 
        DISTINCT b.drn -- Select distinct Driver Reference Numbers (DRN)
    FROM 
        moovebackend.driver_drivervehicleassignment a -- main source table
    JOIN moovebackend.driver_driver b ON a.driver_id = b.id -- Join to get driver details
    WHERE 
        a.effective_end_date IS NULL -- Consider only active assignments where the end date is NULL
),
-- 'driver_assignment' CTE: Get the assignment details for drivers who are NOT currently active
driver_assignment AS (
    SELECT 
        drn,
        city,
        week_no_assigned,
        week_no_unassigned,
        day_last_unassigned,
        Driver_Name
    FROM (
        SELECT 
            b.drn, 
            mc.name AS city, -- City name
            date_trunc('week', min(a.effective_start_date) ::date) AS week_no_assigned,  -- Assignment start week
            date_trunc('week', max(a.effective_end_date) ::date) AS week_no_unassigned, -- Assignment end week,
            (max(a.effective_end_date) ::date) AS day_last_unassigned, -- Assignment end day
            b.name AS Driver_Name -- Driver's Name
--            ROW_NUMBER() OVER (PARTITION BY b.drn ORDER BY a.effective_start_date ASC) as rn, -- Assign a unique row number for each driver based on assignment start date ordered from oldest date to latest date
        FROM 
            moovebackend.driver_drivervehicleassignment a -- Main source table here
        JOIN moovebackend.driver_driver b ON a.driver_id = b.id -- Join to get driver details
        JOIN moovebackend.driver_vehicle c ON a.vehicle_id = c.id  -- Join to get vehicle details--may be needed later
        JOIN moovebackend.markets_city mc ON c.city_id = mc.id  -- Join to get city details
        WHERE 
            a.effective_end_date <= CURRENT_DATE-14 --< '2023-06-26' 
            AND mc.timezone LIKE 'Africa%' 
--            and b.drn NOT IN (SELECT drn FROM active_drivers) -- Exclude active drivers
            group by b.drn, b.name, mc.name
        ) x
    WHERE 
        x.week_no_unassigned IS NOT NULL -- Consider only drivers with end week not null --persistence to ensure all active drivers are ommitted
        and drn = 'MVG10376'
)
-- Final SELECT to return the required output
SELECT 
    a.drn,
    a.city,
    a.week_no_assigned as First_Week_Assigned,
    a.week_no_unassigned as Last_Week_Unassigned,
    a.day_last_unassigned,
    a.Driver_Name,
    SUM(b.gross_booking) as GBs, -- Sum of gross bookings
    SUM(b.perf_trips) as Total_trips, -- Sum of payment trips
    SUM(b.supply_hours) as total_Supply_hrs, -- Sum of supply hours
    SUM(b.km_driven) as total__km_driven, -- Sum of km driven
    MAX(b.week_no) as recent_workweek, -- Most recent work week
    MAX(b.event_date) as recent_workday, -- Most recent work day
    COUNT(b.event_date) as count_of_days_worked_since_last_unassigned ---No. of Days Worked Since Last Day Unassigned
FROM
    driver_assignment a   
LEFT JOIN 
    vw__driver_aggr_daily b
ON 
    a.drn = b.drn -- Join on DRN
WHERE 
    b.event_date between CURRENT_DATE-14 and CURRENT_DATE -->= '2023-06-26' 
    AND b.week_no > a.week_no_unassigned -- Consider  records after '2023-06-19' and for  records post unassignment for each driver
GROUP BY
    a.drn,
    a.city,
    a.week_no_assigned,
    a.week_no_unassigned,
    a.day_last_unassigned,
    a.Driver_Name -- Group by these fields to get sums and maximum
ORDER BY 
    a.city; -- Order the output by city
    



----02: ASSIGNED DRIVERS

-- 'active_drivers' CTE: Get list of currently active drivers
WITH active_drivers AS (
    SELECT 
        DISTINCT b.drn -- Select distinct Driver Reference Numbers (DRN)
    FROM 
        moovebackend.driver_drivervehicleassignment a -- main source table
    JOIN moovebackend.driver_driver b ON a.driver_id = b.id -- Join to get driver details
    WHERE 
        a.effective_end_date IS NULL -- Consider only active assignments where the end date is NULL
),
-- 'driver_assignment' CTE: Get the assignment details for drivers who are NOT currently active Last 7 days
driver_assignment AS (
    SELECT 
        drn,
        city,
        week_no_assigned,
        week_no_unassigned,
        day_last_unassigned,
        Driver_Name
    FROM (
        SELECT 
            b.drn, 
            mc.name AS city, -- City name
            date_trunc('week', a.effective_start_date ::date) AS week_no_assigned,  -- Assignment start week
            date_trunc('week', a.effective_end_date ::date) AS week_no_unassigned, -- Assignment end week,
            (a.effective_end_date ::date) AS day_last_unassigned, -- Assignment end day
            b.name AS Driver_Name, -- Driver's Name
            ROW_NUMBER() OVER (PARTITION BY b.drn ORDER BY a.effective_start_date ASC) as rn -- Assign a unique row number for each driver based on assignment start date ordered from oldest date to latest date 
        FROM 
            moovebackend.driver_drivervehicleassignment a -- Main source table here
        JOIN moovebackend.driver_driver b ON a.driver_id = b.id -- Join to get driver details
        JOIN moovebackend.driver_vehicle c ON a.vehicle_id = c.id  -- Join to get vehicle details--may be needed later
        JOIN moovebackend.markets_city mc ON c.city_id = mc.id  -- Join to get city details
        WHERE 
            a.effective_end_date IS NULL AND mc.timezone LIKE 'Africa%'  
--            and b.drn IN (SELECT drn FROM active_drivers) -- Include only active drivers
        ) x
    WHERE 
        x.rn = 1 AND x.week_no_unassigned IS NULL -- Consider only drivers with end week null --persistence to ensure we capture only assigned drivers
--        and drn = 'DRN001126'
)
-- Final SELECT to return the required output
SELECT 
    a.drn,
    a.city,
    a.week_no_assigned as First_Week_Assigned,
    a.week_no_unassigned as Last_Week_Unassigned,
    a.day_last_unassigned,
    a.Driver_Name,
    SUM(b.gross_booking) as GBs, -- Sum of gross bookings
    SUM(b.perf_trips) as Total_trips, -- Sum of payment trips
    SUM(b.supply_hours) as total_Supply_hrs, -- Sum of supply hours
    SUM(b.km_driven) as total__km_driven, -- Sum of km driven
    MAX(b.week_no) as recent_workweek, -- Most recent work week
    MAX(b.event_date) as recent_workday, -- Most recent work day
    COUNT(b.event_date) as count_of_days_worked_since_last_unassigned ---No. of Days Worked Since Last Day Unassigned
FROM
    driver_assignment a   
LEFT JOIN 
    (select * from public.vw__driver_aggr_daily where event_date between '2023-07-03' and '2023-07-09' --between CURRENT_DATE-7 and CURRENT_DATE 
    )b
ON 
    a.drn = b.drn -- Join on DRN
WHERE 
    (b.perf_trips = 0 or b.perf_trips is null) and a.drn = 'MVS10017'-- Consider  records after '2023-07-03'(last 7days) and for  records with no trips for each driver
GROUP BY
    a.drn,
    a.city,
    a.week_no_assigned,
    a.week_no_unassigned,
    a.day_last_unassigned,
    a.Driver_Name -- Group by these fields to get sums and maximum
ORDER BY 
    a.city; -- Order the output by city
    



