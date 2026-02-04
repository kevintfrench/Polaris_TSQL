/* Polaris Visit Classification Report 
https://forum.innovativeusers.org/t/are-your-patrons-browsing-or-just-picking-up/2734
   Excludes Renewals (SubType 124).
*/

-- 1. Setup Report Parameters
--    The CKO transaction has to happen between these two dates
DECLARE @StartDate DATETIME;
DECLARE @EndDate DATETIME;
DECLARE @NewVisitIntervalMinutes INT = 120; -- 2 Hours determines a "New Visit"

-- Calculate "Previous Month" dynamically
SET @StartDate = DATEADD(month, DATEDIFF(month, 0, GETDATE()) - 1, 0); 
SET @EndDate = DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0);        

-- Cleanup temp tables
IF OBJECT_ID('tempdb..#RawTransactions') IS NOT NULL DROP TABLE #RawTransactions;
IF OBJECT_ID('tempdb..#VisitCalculations') IS NOT NULL DROP TABLE #VisitCalculations;

-- 2. Gather Checkout Transactions (6001)
SELECT 
    th.TransactionID,
    th.OrganizationID,
    o.Name AS BranchName,
    th.TranClientDate,
    MAX(CASE WHEN td.TransactionSubTypeID = 6 THEN td.numValue ELSE NULL END) AS PatronID,
    MAX(CASE WHEN td.TransactionSubTypeID = 233 THEN td.numValue ELSE NULL END) AS HoldRequestID
INTO #RawTransactions
FROM PolarisTransactions.polaris.TransactionHeaders th WITH (NOLOCK)
JOIN Polaris.polaris.Organizations o WITH (NOLOCK) ON th.OrganizationID = o.OrganizationID
JOIN PolarisTransactions.polaris.TransactionDetails td WITH (NOLOCK) 
    ON th.TransactionID = td.TransactionID 
WHERE th.TransactionTypeID = 6001 
AND th.TranClientDate >= @StartDate 
AND th.TranClientDate < @EndDate
AND td.TransactionSubTypeID IN (6, 233, 124) 
GROUP BY 
    th.TransactionID, 
    th.OrganizationID, 
    o.Name, 
    th.TranClientDate
HAVING MAX(CASE WHEN td.TransactionSubTypeID = 124 THEN 1 ELSE 0 END) = 0; -- Exclude Renewals

CREATE INDEX IX_RawTrans_PatronDate ON #RawTransactions(PatronID, TranClientDate);

-- 3. Calculate Visits
SELECT 
    rt.TransactionID,
    rt.OrganizationID,
    rt.BranchName,
    rt.PatronID,
    rt.TranClientDate,
    CASE WHEN rt.HoldRequestID IS NOT NULL THEN 1 ELSE 0 END AS IsHoldCheckout,
    CASE WHEN rt.HoldRequestID IS NULL THEN 1 ELSE 0 END AS IsBrowsedCheckout,
    CASE 
        WHEN DATEDIFF(MINUTE, 
            LAG(rt.TranClientDate, 1, '1900-01-01') OVER (PARTITION BY rt.PatronID, rt.OrganizationID ORDER BY rt.TranClientDate), 
            rt.TranClientDate
        ) > @NewVisitIntervalMinutes -- uses 1900 as the first visit if we can't find a visit before the time period
        THEN 1 
        ELSE 0 
    END AS IsNewVisit
INTO #VisitCalculations
FROM #RawTransactions rt;

-- 4. Aggregate and Classify
;WITH VisitsGrouped AS (
    SELECT 
        OrganizationID,
        BranchName,
        SUM(IsNewVisit) OVER (PARTITION BY PatronID, OrganizationID ORDER BY TranClientDate) AS VisitSequence,
        PatronID,
        IsHoldCheckout,
        IsBrowsedCheckout
    FROM #VisitCalculations
),
VisitSummary AS (
    SELECT 
        OrganizationID,
        BranchName,
        PatronID,
        VisitSequence,
        -- Determine the Nature of the Visit
        CASE 
            WHEN SUM(IsHoldCheckout) > 0 AND SUM(IsBrowsedCheckout) > 0 THEN 'Hybrid'
            WHEN SUM(IsHoldCheckout) > 0 AND SUM(IsBrowsedCheckout) = 0 THEN 'Hold Only'
            WHEN SUM(IsHoldCheckout) = 0 AND SUM(IsBrowsedCheckout) > 0 THEN 'Browse Only'
            ELSE 'Unknown'
        END AS VisitType
    FROM VisitsGrouped
    GROUP BY OrganizationID, BranchName, PatronID, VisitSequence
)
SELECT 
    BranchName AS [Branch],
    COUNT(VisitSequence) AS [Total Visits],
    
    -- Breakdown of Visit Types
    SUM(CASE WHEN VisitType = 'Hold Only' THEN 1 ELSE 0 END) AS [Hold Only Visits],
    SUM(CASE WHEN VisitType = 'Browse Only' THEN 1 ELSE 0 END) AS [Browse Only Visits],
    SUM(CASE WHEN VisitType = 'Hybrid' THEN 1 ELSE 0 END) AS [Hybrid Visits],

    -- Percentages for Context
    CAST((CAST(SUM(CASE WHEN VisitType = 'Hold Only' THEN 1 ELSE 0 END) AS DECIMAL(10,1)) / COUNT(VisitSequence)) * 100 AS DECIMAL(5,1)) AS [% Hold Only],
    CAST((CAST(SUM(CASE WHEN VisitType = 'Browse Only' THEN 1 ELSE 0 END) AS DECIMAL(10,1)) / COUNT(VisitSequence)) * 100 AS DECIMAL(5,1)) AS [% Browse Only],
    CAST((CAST(SUM(CASE WHEN VisitType = 'Hybrid' THEN 1 ELSE 0 END) AS DECIMAL(10,1)) / COUNT(VisitSequence)) * 100 AS DECIMAL(5,1)) AS [% Hybrid]

FROM VisitSummary
GROUP BY BranchName, OrganizationID
ORDER BY BranchName;

-- Cleanup
DROP TABLE #RawTransactions;
DROP TABLE #VisitCalculations;