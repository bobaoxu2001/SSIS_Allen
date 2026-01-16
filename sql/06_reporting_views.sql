/*
================================================================================
Organ Donation ETL Pipeline - Reporting Views and Queries
================================================================================
Script: 06_reporting_views.sql
Purpose: Views and queries supporting operational reporting
Author: Allen Xu
Date: 2025

These queries demonstrate how the data supports real business needs:
- Operational dashboards for transplant coordinators
- Compliance reports for regulatory requirements
- Executive KPIs for management
================================================================================
*/

USE OrganDonationDB;
GO

-- ============================================================================
-- View: Executive Dashboard Summary
-- Purpose: High-level KPIs for leadership
-- ============================================================================
IF OBJECT_ID('dbo.vw_ExecutiveSummary', 'V') IS NOT NULL DROP VIEW dbo.vw_ExecutiveSummary;
GO

CREATE VIEW dbo.vw_ExecutiveSummary
AS
SELECT 
    (SELECT COUNT(*) FROM dbo.Donors WHERE Status = 'Active') AS ActiveDonors,
    (SELECT COUNT(*) FROM dbo.Recipients) AS TotalWaitlistPatients,
    (SELECT COUNT(*) FROM dbo.Recipients WHERE UrgencyCode IN ('1A', '1B')) AS HighUrgencyPatients,
    (SELECT AVG(DaysOnWaitlist) FROM dbo.Recipients) AS AvgDaysOnWaitlist,
    (SELECT COUNT(DISTINCT CenterID) FROM dbo.TransplantCenters WHERE IsActive = 1) AS ActiveCenters,
    (SELECT MAX(ExecutionEndTime) FROM audit.ETL_LoadLog WHERE Status = 'Success') AS LastSuccessfulLoad;
GO

-- ============================================================================
-- View: Donor Availability by Organ and Blood Type
-- Purpose: Supply-side analysis for matching
-- ============================================================================
IF OBJECT_ID('dbo.vw_DonorAvailability', 'V') IS NOT NULL DROP VIEW dbo.vw_DonorAvailability;
GO

CREATE VIEW dbo.vw_DonorAvailability
AS
SELECT 
    d.OrganType,
    d.BloodType,
    COUNT(*) AS TotalDonors,
    SUM(CASE WHEN d.Status = 'Active' THEN 1 ELSE 0 END) AS ActiveDonors,
    SUM(CASE WHEN d.Status = 'Pending' THEN 1 ELSE 0 END) AS PendingDonors,
    SUM(CASE WHEN d.DonorType = 'DBD' THEN 1 ELSE 0 END) AS DBD_Donors,
    SUM(CASE WHEN d.DonorType = 'DCD' THEN 1 ELSE 0 END) AS DCD_Donors
FROM dbo.Donors d
GROUP BY d.OrganType, d.BloodType;
GO

-- ============================================================================
-- View: Waitlist by Urgency and Organ
-- Purpose: Demand-side analysis for capacity planning
-- ============================================================================
IF OBJECT_ID('dbo.vw_WaitlistSummary', 'V') IS NOT NULL DROP VIEW dbo.vw_WaitlistSummary;
GO

CREATE VIEW dbo.vw_WaitlistSummary
AS
SELECT 
    r.NeededOrgan,
    r.UrgencyCode,
    COUNT(*) AS PatientCount,
    AVG(r.DaysOnWaitlist) AS AvgWaitDays,
    MAX(r.DaysOnWaitlist) AS MaxWaitDays,
    MIN(r.DaysOnWaitlist) AS MinWaitDays,
    AVG(r.Age) AS AvgAge
FROM dbo.Recipients r
GROUP BY r.NeededOrgan, r.UrgencyCode;
GO

-- ============================================================================
-- Query: Potential Donor-Recipient Matches
-- Purpose: Simplified matching logic for operational use
-- Note: Real UNOS matching uses complex algorithms (CPRA, distance, etc.)
-- ============================================================================
SELECT 
    d.UNOS_ID AS DonorID,
    d.FirstName + ' ' + d.LastName AS DonorName,
    d.BloodType AS DonorBloodType,
    d.OrganType AS AvailableOrgan,
    d.DonorType,
    dc.OPO_Name AS DonorCenter,
    
    r.UNOS_ID AS RecipientID,
    r.FirstName + ' ' + r.LastName AS RecipientName,
    r.BloodType AS RecipientBloodType,
    r.NeededOrgan,
    r.UrgencyCode,
    r.DaysOnWaitlist,
    rc.OPO_Name AS RecipientCenter,
    
    CASE 
        WHEN d.BloodType = r.BloodType THEN 'Exact Match'
        WHEN d.BloodType = 'O-' THEN 'Universal Donor'
        ELSE 'Compatible'
    END AS MatchType

FROM dbo.Donors d
INNER JOIN dbo.TransplantCenters dc ON d.CenterID = dc.CenterID
INNER JOIN dbo.Recipients r ON d.OrganType = r.NeededOrgan
INNER JOIN dbo.TransplantCenters rc ON r.CenterID = rc.CenterID

WHERE d.Status = 'Active'
  AND d.BloodType = r.BloodType  -- Simplified: exact blood type match only

ORDER BY 
    CASE r.UrgencyCode 
        WHEN '1A' THEN 1 
        WHEN '1B' THEN 2 
        WHEN '2' THEN 3 
        WHEN '3' THEN 4
        ELSE 5
    END,
    r.DaysOnWaitlist DESC;
GO

-- ============================================================================
-- Query: Center Performance Summary
-- Purpose: Regional and center-level metrics
-- ============================================================================
SELECT 
    tc.OPO_Name AS CenterName,
    tc.City,
    tc.State,
    tc.Region AS UNOS_Region,
    tc.CenterType,
    COUNT(DISTINCT d.DonorID) AS TotalDonors,
    SUM(CASE WHEN d.Status = 'Active' THEN 1 ELSE 0 END) AS ActiveDonors,
    COUNT(DISTINCT r.RecipientID) AS TotalRecipients,
    SUM(CASE WHEN r.UrgencyCode IN ('1A', '1B') THEN 1 ELSE 0 END) AS HighUrgencyRecipients
FROM dbo.TransplantCenters tc
LEFT JOIN dbo.Donors d ON tc.CenterID = d.CenterID
LEFT JOIN dbo.Recipients r ON tc.CenterID = r.CenterID
WHERE tc.IsActive = 1
GROUP BY tc.OPO_Name, tc.City, tc.State, tc.Region, tc.CenterType
ORDER BY COUNT(DISTINCT d.DonorID) + COUNT(DISTINCT r.RecipientID) DESC;
GO

-- ============================================================================
-- Query: Blood Type Supply-Demand Analysis
-- Purpose: Identify shortages and surpluses
-- ============================================================================
SELECT 
    bt.BloodTypeName,
    COALESCE(donors.ActiveCount, 0) AS AvailableDonors,
    COALESCE(recipients.WaitingCount, 0) AS WaitingRecipients,
    COALESCE(recipients.WaitingCount, 0) - COALESCE(donors.ActiveCount, 0) AS Gap,
    CASE 
        WHEN COALESCE(donors.ActiveCount, 0) = 0 THEN 'Critical Shortage'
        WHEN COALESCE(recipients.WaitingCount, 0) > COALESCE(donors.ActiveCount, 0) * 3 THEN 'Severe Shortage'
        WHEN COALESCE(recipients.WaitingCount, 0) > COALESCE(donors.ActiveCount, 0) THEN 'Shortage'
        ELSE 'Adequate'
    END AS SupplyStatus
FROM dbo.BloodTypes bt
LEFT JOIN (
    SELECT BloodType, COUNT(*) AS ActiveCount
    FROM dbo.Donors WHERE Status = 'Active'
    GROUP BY BloodType
) donors ON bt.BloodTypeCode = donors.BloodType
LEFT JOIN (
    SELECT BloodType, COUNT(*) AS WaitingCount
    FROM dbo.Recipients
    GROUP BY BloodType
) recipients ON bt.BloodTypeCode = recipients.BloodType
ORDER BY bt.BloodTypeCode;
GO

-- ============================================================================
-- Query: ETL Health Dashboard
-- Purpose: Monitor data pipeline performance
-- ============================================================================
SELECT 
    LoadID,
    PackageName,
    ExecutionStartTime,
    ExecutionEndTime,
    DATEDIFF(SECOND, ExecutionStartTime, ExecutionEndTime) AS DurationSeconds,
    Status,
    SourceRowCount,
    StagingRowCount,
    InsertedRowCount,
    ErrorRowCount,
    CAST(
        CASE WHEN SourceRowCount > 0 
             THEN ErrorRowCount * 100.0 / SourceRowCount 
             ELSE 0 
        END AS DECIMAL(5,2)
    ) AS ErrorRate,
    ExecutedBy
FROM audit.ETL_LoadLog
WHERE ExecutionStartTime >= DATEADD(DAY, -7, GETDATE())
ORDER BY ExecutionStartTime DESC;
GO

-- ============================================================================
-- Query: Data Quality Error Analysis
-- Purpose: Identify error patterns for source system improvement
-- ============================================================================
SELECT 
    'Donors' AS EntityType,
    ErrorCode,
    ErrorColumn,
    ErrorDescription,
    COUNT(*) AS ErrorCount
FROM error.Donors
WHERE ErrorDateTime >= DATEADD(DAY, -30, GETDATE())
GROUP BY ErrorCode, ErrorColumn, ErrorDescription

UNION ALL

SELECT 
    'Recipients' AS EntityType,
    ErrorCode,
    ErrorColumn,
    ErrorDescription,
    COUNT(*) AS ErrorCount
FROM error.Recipients
WHERE ErrorDateTime >= DATEADD(DAY, -30, GETDATE())
GROUP BY ErrorCode, ErrorColumn, ErrorDescription

ORDER BY ErrorCount DESC;
GO

PRINT 'Reporting views and queries created successfully.';
GO
