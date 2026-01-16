/*
================================================================================
Organ Donation ETL Pipeline - Manual ETL Test Script
================================================================================
Script: 07_manual_etl_test.sql
Purpose: Simulates complete ETL flow without SSIS (for testing and demo)
Author: [Your Name]
Date: 2025

Usage: Run this script in SSMS to test the entire ETL flow using pure T-SQL.
This is useful for:
1. Validating SQL logic before building SSIS packages
2. Quick demonstrations without SSIS environment
3. Debugging validation rules
================================================================================
*/

USE OrganDonationDB;
GO

PRINT '========================================';
PRINT '  ORGAN DONATION ETL - MANUAL TEST     ';
PRINT '========================================';
PRINT '';

-- ============================================================================
-- STEP 1: Initialize Audit Log
-- ============================================================================
PRINT '[Step 1/7] Starting ETL audit log...';

DECLARE @LoadID INT;
EXEC audit.usp_StartETLLoad 
    @PackageName = 'ManualETLTest',
    @SourceFileName = 'All CSV files',
    @LoadID = @LoadID OUTPUT;

PRINT '         LoadID: ' + CAST(@LoadID AS VARCHAR(10));
PRINT '';

-- ============================================================================
-- STEP 2: Load Transplant Centers (Reference Data)
-- ============================================================================
PRINT '[Step 2/7] Loading Transplant Centers...';

TRUNCATE TABLE stg.TransplantCenters;

INSERT INTO stg.TransplantCenters (OPO_Code, OPO_Name, City, State, Region, CenterType, CMS_Certification, AccreditationDate, Phone, Email, LoadID, SourceFileName, SourceRowNumber)
VALUES 
('NYRT', 'New York Regional Transplant Program', 'New York', 'NY', '9', 'Comprehensive', 'NY0001', '2018-06-15', '212-555-0100', 'transplant@nyrt.org', @LoadID, 'transplant_centers.csv', 1),
('CAOP', 'California Organ Procurement', 'Los Angeles', 'CA', '5', 'Comprehensive', 'CA0001', '2017-03-22', '310-555-0200', 'ops@caop.org', @LoadID, 'transplant_centers.csv', 2),
('TXGC', 'Texas Gulf Coast Transplant', 'Houston', 'TX', '4', 'Comprehensive', 'TX0001', '2019-09-10', '713-555-0300', 'transplant@txgc.org', @LoadID, 'transplant_centers.csv', 3),
('FLWC', 'Florida West Coast OPO', 'Tampa', 'FL', '3', 'Comprehensive', 'FL0001', '2020-01-18', '813-555-0400', 'referral@flwc.org', @LoadID, 'transplant_centers.csv', 4),
('ILOP', 'Illinois Organ Procurement', 'Chicago', 'IL', '7', 'Comprehensive', 'IL0001', '2016-11-05', '312-555-0500', 'opo@ilop.org', @LoadID, 'transplant_centers.csv', 5),
('OHOP', 'Ohio Organ Procurement', 'Columbus', 'OH', '10', 'Kidney-Liver', 'OH0001', '2021-04-28', '614-555-0600', 'center@ohop.org', @LoadID, 'transplant_centers.csv', 6);

-- Load to production
MERGE dbo.TransplantCenters AS target
USING (
    SELECT OPO_Code, OPO_Name, City, LEFT(State, 2) AS State, 
           TRY_CAST(Region AS TINYINT) AS Region, CenterType,
           CMS_Certification, TRY_CAST(AccreditationDate AS DATE) AS AccreditationDate,
           Phone, Email
    FROM stg.TransplantCenters WHERE LoadID = @LoadID
) AS source ON target.OPO_Code = source.OPO_Code
WHEN MATCHED THEN UPDATE SET OPO_Name = source.OPO_Name, ModifiedDate = GETDATE()
WHEN NOT MATCHED THEN INSERT (OPO_Code, OPO_Name, City, State, Region, CenterType, CMS_Certification, AccreditationDate, Phone, Email)
    VALUES (source.OPO_Code, source.OPO_Name, source.City, source.State, source.Region, source.CenterType, source.CMS_Certification, source.AccreditationDate, source.Phone, source.Email);

PRINT '         Centers loaded: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
PRINT '';

-- ============================================================================
-- STEP 3: Load Donor Staging Data
-- ============================================================================
PRINT '[Step 3/7] Loading Donors to Staging...';

TRUNCATE TABLE stg.Donors;
DELETE FROM error.Donors WHERE LoadID = @LoadID;

-- Sample data including intentional errors for validation testing
INSERT INTO stg.Donors (UNOS_ID, FirstName, LastName, DateOfBirth, BloodType, OrganType, ReferralDate, OPO_Code, DonorType, Status, CauseOfDeath, Height_cm, Weight_kg, ContactPhone, LoadID, SourceFileName, SourceRowNumber)
VALUES 
('ABJK-78234', 'Michael', 'Patterson', '1985-03-15', 'O+', 'Kidney', '2024-01-15', 'NYRT', 'DBD', 'Active', 'CVA', '178', '82', '555-0101', @LoadID, 'donors.csv', 1),
('CDMN-45892', 'Sarah', 'Mitchell', '1990-07-22', 'A-', 'Liver', '2024-02-20', 'CAOP', 'DBD', 'Active', 'Head Trauma', '165', '68', '555-0102', @LoadID, 'donors.csv', 2),
('EFPQ-12456', 'James', 'Rodriguez', '1978-11-08', 'B+', 'Heart', '2024-01-30', 'TXGC', 'DCD', 'Inactive', 'Anoxia', '180', '90', '555-0103', @LoadID, 'donors.csv', 3),
('GHRS-89123', 'Emily', 'Thompson', '1995-04-12', 'AB+', 'Kidney', '2024-03-10', 'FLWC', 'DBD', 'Active', 'CVA', '170', '65', '555-0104', @LoadID, 'donors.csv', 4),
('IJTU-56789', 'Robert', 'Anderson', '1982-09-25', 'O-', 'Lung', '2024-02-05', 'ILOP', 'DBD', 'Active', 'Head Trauma', '175', '78', '555-0105', @LoadID, 'donors.csv', 5),
('KLVW-23456', 'Jennifer', 'Martinez', '1988-12-03', 'A+', 'Liver', '2024-03-22', 'NYRT', 'DCD', 'Pending', 'CVA', '163', '62', '555-0106', @LoadID, 'donors.csv', 6),
('MNXY-90123', 'William', 'Garcia', '1975-06-18', 'B-', 'Kidney', '2024-01-25', 'OHOP', 'DBD', 'Active', 'Anoxia', '182', '88', '555-0107', @LoadID, 'donors.csv', 7),
('OPZA-67890', 'Linda', 'Wilson', '1992-02-28', 'O+', 'Heart', '2024-04-01', 'CAOP', 'DBD', 'Active', 'Head Trauma', '168', '70', '555-0108', @LoadID, 'donors.csv', 8),
-- Error test cases:
('XXXX-00001', 'Test', 'Invalid', NULL, 'O+', 'Kidney', '2024-01-15', 'NYRT', 'DBD', 'Active', 'CVA', '178', '82', '555-9901', @LoadID, 'donors.csv', 31),
('YYYY-00002', 'Bad', 'BloodType', '1985-03-15', 'X+', 'Liver', '2024-02-20', 'CAOP', 'DBD', 'Active', 'CVA', '165', '68', '555-9902', @LoadID, 'donors.csv', 32),
('ZZZZ-00003', 'Future', 'Birth', '2030-07-22', 'A+', 'Heart', '2024-03-10', 'TXGC', 'DBD', 'Active', 'CVA', '170', '65', '555-9903', @LoadID, 'donors.csv', 33),
('AAAA-00004', 'Missing', 'OPO', '1978-11-08', 'B+', 'Kidney', '2024-01-30', 'INVALID', 'DBD', 'Active', 'CVA', '180', '90', '555-9904', @LoadID, 'donors.csv', 34),
('BBBB-00005', 'Bad', 'Status', '1982-09-25', 'O-', 'Lung', '2024-02-05', 'NYRT', 'DBD', 'Unknown', 'CVA', '175', '78', '555-9905', @LoadID, 'donors.csv', 35);

DECLARE @DonorStagedCount INT = @@ROWCOUNT;
PRINT '         Donors staged: ' + CAST(@DonorStagedCount AS VARCHAR(10));
PRINT '';

-- ============================================================================
-- STEP 4: Validate Donors
-- ============================================================================
PRINT '[Step 4/7] Validating Donor records...';
EXEC dbo.usp_ValidateDonors @LoadID = @LoadID;

DECLARE @DonorErrorCount INT = (SELECT COUNT(*) FROM error.Donors WHERE LoadID = @LoadID);
PRINT '         Validation errors: ' + CAST(@DonorErrorCount AS VARCHAR(10));
PRINT '';

-- ============================================================================
-- STEP 5: Load Valid Donors to Production
-- ============================================================================
PRINT '[Step 5/7] Loading valid Donors to Production...';
EXEC dbo.usp_LoadValidDonors @LoadID = @LoadID;

DECLARE @DonorLoadedCount INT = (SELECT COUNT(*) FROM dbo.Donors WHERE LoadID = @LoadID);
PRINT '         Donors loaded: ' + CAST(@DonorLoadedCount AS VARCHAR(10));
PRINT '';

-- ============================================================================
-- STEP 6: Load Recipients (Similar Pattern)
-- ============================================================================
PRINT '[Step 6/7] Loading Recipients...';

TRUNCATE TABLE stg.Recipients;
DELETE FROM error.Recipients WHERE LoadID = @LoadID;

INSERT INTO stg.Recipients (UNOS_ID, FirstName, LastName, DateOfBirth, BloodType, NeededOrgan, ListingDate, OPO_Code, Status, UrgencyCode, Diagnosis, Height_cm, Weight_kg, PRA_Percent, LoadID, SourceFileName, SourceRowNumber)
VALUES 
('RCPT-10001', 'Alice', 'Cooper', '1965-08-12', 'O+', 'Kidney', '2023-06-15', 'NYRT', '1', '1A', 'ESRD - Diabetes', '162', '70', '15', @LoadID, 'recipients.csv', 1),
('RCPT-10002', 'George', 'Foster', '1958-03-25', 'A-', 'Liver', '2023-09-20', 'CAOP', '1', '1B', 'Cirrhosis - HCV', '175', '82', '8', @LoadID, 'recipients.csv', 2),
('RCPT-10003', 'Helen', 'Baker', '1972-11-30', 'B+', 'Heart', '2023-04-10', 'TXGC', '1', '1A', 'Cardiomyopathy', '168', '65', '22', @LoadID, 'recipients.csv', 3),
('RCPT-10004', 'Frank', 'Nelson', '1980-07-18', 'AB+', 'Kidney', '2023-12-05', 'FLWC', '1', '2', 'ESRD - PKD', '180', '88', '35', @LoadID, 'recipients.csv', 4),
('RCPT-10005', 'Grace', 'Howard', '1955-02-14', 'O-', 'Lung', '2023-08-22', 'ILOP', '1', '1B', 'COPD', '158', '55', '12', @LoadID, 'recipients.csv', 5),
('RCPT-10006', 'Henry', 'Morgan', '1968-09-08', 'A+', 'Liver', '2024-01-15', 'OHOP', '1', '2', 'NASH Cirrhosis', '182', '95', '5', @LoadID, 'recipients.csv', 6),
-- Error test cases:
('RCPT-90001', 'Invalid', 'Recipient', NULL, 'O+', 'Kidney', '2023-06-15', 'NYRT', '1', '1A', 'ESRD', '162', '70', '15', @LoadID, 'recipients.csv', 26),
('RCPT-90002', 'BadBlood', 'Type', '1965-08-12', 'Z-', 'Liver', '2023-09-20', 'CAOP', '1', '1B', 'Cirrhosis', '175', '82', '8', @LoadID, 'recipients.csv', 27),
('RCPT-90003', 'Future', 'Listing', '1972-11-30', 'B+', 'Heart', '2030-04-10', 'TXGC', '1', '1A', 'CHF', '168', '65', '22', @LoadID, 'recipients.csv', 28),
('RCPT-90004', 'Invalid', 'OPO', '1980-07-18', 'AB+', 'Kidney', '2023-12-05', 'BADCODE', '1', '2', 'ESRD', '180', '88', '35', @LoadID, 'recipients.csv', 29);

DECLARE @RecipientStagedCount INT = @@ROWCOUNT;
PRINT '         Recipients staged: ' + CAST(@RecipientStagedCount AS VARCHAR(10));

EXEC dbo.usp_ValidateRecipients @LoadID = @LoadID;
DECLARE @RecipientErrorCount INT = (SELECT COUNT(*) FROM error.Recipients WHERE LoadID = @LoadID);
PRINT '         Validation errors: ' + CAST(@RecipientErrorCount AS VARCHAR(10));

EXEC dbo.usp_LoadValidRecipients @LoadID = @LoadID;
DECLARE @RecipientLoadedCount INT = (SELECT COUNT(*) FROM dbo.Recipients WHERE LoadID = @LoadID);
PRINT '         Recipients loaded: ' + CAST(@RecipientLoadedCount AS VARCHAR(10));
PRINT '';

-- ============================================================================
-- STEP 7: Complete Audit Log
-- ============================================================================
PRINT '[Step 7/7] Completing audit log...';

EXEC audit.usp_CompleteETLLoad 
    @LoadID = @LoadID,
    @Status = 'Success',
    @SourceRowCount = 23,  -- 13 donors + 10 recipients
    @StagingRowCount = 23,
    @InsertedRowCount = 14, -- 8 donors + 6 recipients (valid)
    @ErrorRowCount = 9;     -- 5 donor errors + 4 recipient errors

PRINT '';
PRINT '========================================';
PRINT '  ETL COMPLETE - VERIFICATION RESULTS  ';
PRINT '========================================';
PRINT '';

-- ============================================================================
-- VERIFICATION RESULTS
-- ============================================================================
PRINT '>> Audit Log Entry:';
SELECT LoadID, PackageName, Status, 
       SourceRowCount, InsertedRowCount, ErrorRowCount,
       DATEDIFF(SECOND, ExecutionStartTime, ExecutionEndTime) AS DurationSec
FROM audit.ETL_LoadLog WHERE LoadID = @LoadID;

PRINT '';
PRINT '>> Donor Errors Captured:';
SELECT UNOS_ID, ErrorCode, ErrorColumn, ErrorDescription 
FROM error.Donors WHERE LoadID = @LoadID;

PRINT '';
PRINT '>> Recipient Errors Captured:';
SELECT UNOS_ID, ErrorCode, ErrorColumn, ErrorDescription 
FROM error.Recipients WHERE LoadID = @LoadID;

PRINT '';
PRINT '>> Executive Summary:';
SELECT * FROM dbo.vw_ExecutiveSummary;

PRINT '';
PRINT '>> Active Donors:';
SELECT UNOS_ID, FirstName, LastName, BloodType, OrganType, Status 
FROM dbo.Donors WHERE Status = 'Active';

PRINT '';
PRINT '>> High Urgency Recipients:';
SELECT UNOS_ID, FirstName, LastName, BloodType, NeededOrgan, UrgencyCode, DaysOnWaitlist
FROM dbo.Recipients WHERE UrgencyCode IN ('1A', '1B')
ORDER BY UrgencyCode, DaysOnWaitlist DESC;

PRINT '';
PRINT '========================================';
PRINT '  MANUAL ETL TEST COMPLETED            ';
PRINT '========================================';
GO
