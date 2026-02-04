/* ==========================================================
   POLARIS TRANSIT TIME ANALYZER - https://forum.innovativeusers.org/t/polaris-travel-time-analyzer/2733
   ========================================================== */

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; -- Prevents blocking Circulation staff

/* --- PARAMETERS --- */
DECLARE @LookbackDays INT = 365; --Trips that start BEFORE or after this trip will be excluded
DECLARE @MinMinutes INT = 60; --Trips LESS than these many minutes will be excluded
DECLARE @MaxTravelDays DECIMAL(10,4) = 14; --Trips longer than this will be considered Too Long; they will be excluded from the average trip length, but a count will be shown
DECLARE @MinTripCount INT = 5; --Locations pairs must have had at least these many trips between them to be included
DECLARE @OutlierThresholdPct DECIMAL(4,2) = 0.10; --If more than this % of trips are outside the max travel days threshold, then the pair will be considered outliers
	
-- SAMPLING of Outlier locations
DECLARE @SampleSize INT = 3;                
DECLARE @SampleMethod VARCHAR(10) = 'LONGEST'; --LONGEST or RANDOM supported

-- Pre-calculate Date to avoid function calls in WHERE clause
DECLARE @StartDate DATETIME = DATEADD(DAY, -@LookbackDays, GETDATE());

/* --- CONFIGURATION --- */
-- 1. Excluded Branch Name Patterns
DECLARE @Exclude TABLE (Pattern VARCHAR(100));
INSERT INTO @Exclude (Pattern) VALUES
    ('%clc%'), ('%outreach%'), ('%zzz%'), ('%school%'), ('%tech%'),
    ('%float%'), ('%locker%'), ('%drive%'), ('%cart%'), ('%annex%'),
    ('%student%'), ('%on-the-go%'), ('%pickup%'), ('%homebound%'),
    ('%junior high%'), ('%elementary%'), ('%pop up%'), ('%kiosk%'),
    ('%pop-up%'), ('% ill%'), ('%operations%'), ('%check out%'),
    ('%processing%'), ('%mobile%'), ('%central library%');

-- 2. Excluded Statuses (To filter out noise)
-- 16: Unavailable
DECLARE @ExcludeStatuses TABLE (ID INT);
INSERT INTO @ExcludeStatuses (ID) VALUES (16); 

-- 3. Trip Start Triggers
-- 6: In-Transit, 5: Transferred
DECLARE @TripStartStatuses TABLE (ID INT);
INSERT INTO @TripStartStatuses (ID) VALUES (6), (5); 

/* --- EXECUTION --- */

-- Clean up temp table if exists
DROP TABLE IF EXISTS #TripLegs;

;WITH ValidOrgs AS (
    SELECT OrganizationID, Name
    FROM Polaris.Polaris.Organizations o
    WHERE NOT EXISTS (SELECT 1 FROM @Exclude e WHERE o.Name LIKE e.Pattern)
),
RawHistory AS (
    -- Step 1: Grab minimal data, applying filters early
    SELECT 
        h.ItemRecordID,
        h.TransactionDate,
        h.OrganizationID,
        h.ItemRecordHistoryID,
        h.NewItemStatusID
    FROM Polaris.Polaris.ItemRecordHistory h
    WHERE h.TransactionDate >= @StartDate
      -- Filter out excluded statuses early to keep the window function light
      AND NOT EXISTS (SELECT 1 FROM @ExcludeStatuses es WHERE es.ID = h.OldItemStatusID OR es.ID = h.NewItemStatusID)
),
CalculatedLegs AS (
    -- Step 2: We look ahead to find the 'Next' scan for this item
    SELECT 
        rh.ItemRecordID,
        rh.TransactionDate AS FromDate,
        LEAD(rh.TransactionDate) OVER (PARTITION BY rh.ItemRecordID ORDER BY rh.TransactionDate, rh.ItemRecordHistoryID) AS ToDate,
        rh.OrganizationID AS FromOrgID,
        LEAD(rh.OrganizationID) OVER (PARTITION BY rh.ItemRecordID ORDER BY rh.TransactionDate, rh.ItemRecordHistoryID) AS ToOrgID,
        rh.NewItemStatusID
    FROM RawHistory rh
)
SELECT 
    cl.ItemRecordID,
    cl.FromOrgID,
    cl.ToOrgID,
    DATEDIFF(MINUTE, cl.FromDate, cl.ToDate) AS TravelMinutes,
    CAST(DATEDIFF(MINUTE, cl.FromDate, cl.ToDate) / 1440.0 AS DECIMAL(10,4)) AS TravelDays,
    fo.Name AS FromOrgName,
    to2.Name AS ToOrgName
INTO #TripLegs
FROM CalculatedLegs cl
-- Inner joins here act as filters. 
-- If the Org was excluded in the CTE, the row drops out here.
JOIN ValidOrgs fo  ON cl.FromOrgID = fo.OrganizationID
JOIN ValidOrgs to2 ON cl.ToOrgID = to2.OrganizationID
JOIN @TripStartStatuses startStatus ON cl.NewItemStatusID = startStatus.ID
WHERE 
      cl.ToDate IS NOT NULL        -- Trip must have finished
  AND cl.FromOrgID <> cl.ToOrgID   -- Must have changed location (actual transit)
  AND DATEDIFF(MINUTE, cl.FromDate, cl.ToDate) >= @MinMinutes;

/* --- INDEXING --- */
-- Creating indexes on temp tables is best practice for aggregation performance
CREATE CLUSTERED INDEX IX_TripLegs_Orgs ON #TripLegs (FromOrgID, ToOrgID);
CREATE NONCLUSTERED INDEX IX_TripLegs_Stats ON #TripLegs (TravelDays) INCLUDE (ItemRecordID);

/* --- REPORTING --- */
SELECT
      t.FromOrgID
    , t.FromOrgName
    , t.ToOrgID
    , t.ToOrgName
    
    -- Metrics
    , SUM(CASE WHEN t.TravelDays > @MaxTravelDays THEN 1 ELSE 0 END) AS [Count > MaxDays]
    , SUM(CASE WHEN t.TravelDays <= @MaxTravelDays THEN 1 ELSE 0 END) AS [Count OK]
    , AVG(CASE WHEN t.TravelDays <= @MaxTravelDays THEN t.TravelDays ELSE NULL END) AS [Avg Days (Valid Only)]

    -- We multiply by 100.0 to force non-integer division and get a clean percentage
    , CAST(
        (100.0 * SUM(CASE WHEN t.TravelDays > @MaxTravelDays THEN 1 ELSE 0 END)) 
        / COUNT(*) 
      AS DECIMAL(5,1)) AS [% Too Long Trips]

    -- Outlier Flag
    , CASE 
        WHEN (1.0 * SUM(CASE WHEN t.TravelDays > @MaxTravelDays THEN 1 ELSE 0 END)) 
             / COUNT(*) > @OutlierThresholdPct 
        THEN 'HIGH OUTLIERS' ELSE '' 
      END AS Status

    -- Sampling
    , (
        SELECT STRING_AGG(SampleInfo, ', ') WITHIN GROUP (ORDER BY DaysTaken DESC)
        FROM (
            SELECT TOP (@SampleSize) 
                CONCAT(sub.ItemRecordID, ' (', FORMAT(sub.TravelDays, 'N1'), 'd)') AS SampleInfo,
                sub.TravelDays AS DaysTaken
            FROM #TripLegs sub
            WHERE sub.FromOrgID = t.FromOrgID 
              AND sub.ToOrgID = t.ToOrgID
              AND sub.TravelDays > @MaxTravelDays 
            ORDER BY 
                CASE WHEN @SampleMethod = 'LONGEST' THEN sub.TravelDays END DESC,
                CASE WHEN @SampleMethod = 'RANDOM'  THEN CHECKSUM(NEWID()) END 
        ) AS TopN
      ) AS [Examples (ItemID + Days)]

FROM #TripLegs t
GROUP BY t.FromOrgID, t.FromOrgName, t.ToOrgID, t.ToOrgName
HAVING SUM(CASE WHEN t.TravelDays <= @MaxTravelDays THEN 1 ELSE 0 END) >= @MinTripCount
-- Order by the new Failure Rate column descending to put the worst routes at the top
ORDER BY [% Too Long Trips] DESC, t.FromOrgName;

-- Clean up
DROP TABLE #TripLegs;