/*
================================================================================
Organ Donation ETL Pipeline - Validation Stored Procedures
================================================================================
Script: 05_validation_procs.sql
Purpose: Data validation and production load procedures
Author: Allen Xu
Date: 2025

Validation Categories:
- VLD001-VLD010: Completeness checks (required fields)
- VLD011-VLD020: Validity checks (format, range)
- VLD021-VLD030: Referential integrity checks
- VLD031-VLD040: Business rule checks
================================================================================
*/

USE OrganDonationDB;
GO

-- ============================================================================
-- Procedure: Validate Donor Records
-- Validates staging data and routes errors to error.Donors
-- ============================================================================
IF OBJECT_ID('dbo.usp_ValidateDonors', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_ValidateDonors;
GO

CREATE PROCEDURE dbo.usp_ValidateDonors
    @LoadID INT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ErrorCount INT = 0;
    
    -- ========================================================================
    -- VLD001: Required Fields - UNOS_ID, FirstName, LastName
    -- Business Rule: All donors must have identification
    -- ========================================================================
    INSERT INTO error.Donors (
        LoadID, UNOS_ID, FirstName, LastName, DateOfBirth, BloodType,
        OrganType, ReferralDate, OPO_Code, DonorType, Status, CauseOfDeath,
        Height_cm, Weight_kg, ContactPhone,
        ErrorCode, ErrorColumn, ErrorDescription, SourceRowNumber
    )
    SELECT 
        @LoadID, UNOS_ID, FirstName, LastName, DateOfBirth, BloodType,
        OrganType, ReferralDate, OPO_Code, DonorType, Status, CauseOfDeath,
        Height_cm, Weight_kg, ContactPhone,
        'VLD001', 'UNOS_ID/Name',
        'Required field is NULL or empty. UNOS_ID, FirstName, and LastName are mandatory.',
        SourceRowNumber
    FROM stg.Donors
    WHERE LoadID = @LoadID
      AND (UNOS_ID IS NULL OR LTRIM(RTRIM(UNOS_ID)) = ''
           OR FirstName IS NULL OR LTRIM(RTRIM(FirstName)) = ''
           OR LastName IS NULL OR LTRIM(RTRIM(LastName)) = '');
    
    SET @ErrorCount = @ErrorCount + @@ROWCOUNT;
    
    -- ========================================================================
    -- VLD002: Blood Type Validation
    -- Must be valid ABO/Rh format
    -- ========================================================================
    INSERT INTO error.Donors (
        LoadID, UNOS_ID, FirstName, LastName, DateOfBirth, BloodType,
        OrganType, ReferralDate, OPO_Code, DonorType, Status, CauseOfDeath,
        Height_cm, Weight_kg, ContactPhone,
        ErrorCode, ErrorColumn, ErrorDescription, SourceRowNumber
    )
    SELECT 
        @LoadID, UNOS_ID, FirstName, LastName, DateOfBirth, BloodType,
        OrganType, ReferralDate, OPO_Code, DonorType, Status, CauseOfDeath,
        Height_cm, Weight_kg, ContactPhone,
        'VLD002', 'BloodType',
        'Invalid blood type: ''' + COALESCE(BloodType, 'NULL') + '''. Valid values: O+, O-, A+, A-, B+, B-, AB+, AB-',
        SourceRowNumber
    FROM stg.Donors s
    WHERE LoadID = @LoadID
      AND UNOS_ID NOT IN (SELECT UNOS_ID FROM error.Donors WHERE LoadID = @LoadID)
      AND NOT EXISTS (
          SELECT 1 FROM dbo.BloodTypes bt 
          WHERE bt.BloodTypeCode = s.BloodType AND bt.IsActive = 1
      );
    
    SET @ErrorCount = @ErrorCount + @@ROWCOUNT;
    
    -- ========================================================================
    -- VLD003: Date of Birth Validation
    -- Must be valid date, age between 0-120
    -- ========================================================================
    INSERT INTO error.Donors (
        LoadID, UNOS_ID, FirstName, LastName, DateOfBirth, BloodType,
        OrganType, ReferralDate, OPO_Code, DonorType, Status, CauseOfDeath,
        Height_cm, Weight_kg, ContactPhone,
        ErrorCode, ErrorColumn, ErrorDescription, SourceRowNumber
    )
    SELECT 
        @LoadID, UNOS_ID, FirstName, LastName, DateOfBirth, BloodType,
        OrganType, ReferralDate, OPO_Code, DonorType, Status, CauseOfDeath,
        Height_cm, Weight_kg, ContactPhone,
        'VLD003', 'DateOfBirth',
        CASE 
            WHEN DateOfBirth IS NULL OR TRY_CAST(DateOfBirth AS DATE) IS NULL 
                THEN 'Invalid or missing date of birth: ''' + COALESCE(DateOfBirth, 'NULL') + ''''
            WHEN TRY_CAST(DateOfBirth AS DATE) > GETDATE() 
                THEN 'Date of birth cannot be in the future: ' + DateOfBirth
            ELSE 'Age exceeds valid range (0-120 years)'
        END,
        SourceRowNumber
    FROM stg.Donors s
    WHERE LoadID = @LoadID
      AND UNOS_ID NOT IN (SELECT UNOS_ID FROM error.Donors WHERE LoadID = @LoadID)
      AND (DateOfBirth IS NULL 
           OR TRY_CAST(DateOfBirth AS DATE) IS NULL
           OR TRY_CAST(DateOfBirth AS DATE) > GETDATE()
           OR DATEDIFF(YEAR, TRY_CAST(DateOfBirth AS DATE), GETDATE()) > 120);
    
    SET @ErrorCount = @ErrorCount + @@ROWCOUNT;
    
    -- ========================================================================
    -- VLD004: OPO Code Referential Integrity
    -- Must exist in TransplantCenters table
    -- ========================================================================
    INSERT INTO error.Donors (
        LoadID, UNOS_ID, FirstName, LastName, DateOfBirth, BloodType,
        OrganType, ReferralDate, OPO_Code, DonorType, Status, CauseOfDeath,
        Height_cm, Weight_kg, ContactPhone,
        ErrorCode, ErrorColumn, ErrorDescription, SourceRowNumber
    )
    SELECT 
        @LoadID, UNOS_ID, FirstName, LastName, DateOfBirth, BloodType,
        OrganType, ReferralDate, OPO_Code, DonorType, Status, CauseOfDeath,
        Height_cm, Weight_kg, ContactPhone,
        'VLD004', 'OPO_Code',
        'OPO code not found in TransplantCenters: ''' + COALESCE(OPO_Code, 'NULL') + '''',
        SourceRowNumber
    FROM stg.Donors s
    WHERE LoadID = @LoadID
      AND UNOS_ID NOT IN (SELECT UNOS_ID FROM error.Donors WHERE LoadID = @LoadID)
      AND NOT EXISTS (
          SELECT 1 FROM dbo.TransplantCenters tc 
          WHERE tc.OPO_Code = s.OPO_Code AND tc.IsActive = 1
      );
    
    SET @ErrorCount = @ErrorCount + @@ROWCOUNT;
    
    -- ========================================================================
    -- VLD005: Status Validation
    -- Must be valid donor status
    -- ========================================================================
    INSERT INTO error.Donors (
        LoadID, UNOS_ID, FirstName, LastName, DateOfBirth, BloodType,
        OrganType, ReferralDate, OPO_Code, DonorType, Status, CauseOfDeath,
        Height_cm, Weight_kg, ContactPhone,
        ErrorCode, ErrorColumn, ErrorDescription, SourceRowNumber
    )
    SELECT 
        @LoadID, UNOS_ID, FirstName, LastName, DateOfBirth, BloodType,
        OrganType, ReferralDate, OPO_Code, DonorType, Status, CauseOfDeath,
        Height_cm, Weight_kg, ContactPhone,
        'VLD005', 'Status',
        'Invalid status: ''' + COALESCE(Status, 'NULL') + '''. Valid values: Active, Inactive, Pending',
        SourceRowNumber
    FROM stg.Donors s
    WHERE LoadID = @LoadID
      AND UNOS_ID NOT IN (SELECT UNOS_ID FROM error.Donors WHERE LoadID = @LoadID)
      AND Status NOT IN ('Active', 'Inactive', 'Pending');
    
    SET @ErrorCount = @ErrorCount + @@ROWCOUNT;
    
    -- Return summary
    SELECT @ErrorCount AS TotalValidationErrors;
    PRINT 'Donor validation complete. Errors found: ' + CAST(@ErrorCount AS VARCHAR(10));
END
GO

-- ============================================================================
-- Procedure: Validate Recipient Records
-- ============================================================================
IF OBJECT_ID('dbo.usp_ValidateRecipients', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_ValidateRecipients;
GO

CREATE PROCEDURE dbo.usp_ValidateRecipients
    @LoadID INT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ErrorCount INT = 0;
    
    -- VLD001: Required Fields
    INSERT INTO error.Recipients (
        LoadID, UNOS_ID, FirstName, LastName, DateOfBirth, BloodType,
        NeededOrgan, ListingDate, OPO_Code, Status, UrgencyCode, Diagnosis,
        Height_cm, Weight_kg, PRA_Percent,
        ErrorCode, ErrorColumn, ErrorDescription, SourceRowNumber
    )
    SELECT 
        @LoadID, UNOS_ID, FirstName, LastName, DateOfBirth, BloodType,
        NeededOrgan, ListingDate, OPO_Code, Status, UrgencyCode, Diagnosis,
        Height_cm, Weight_kg, PRA_Percent,
        'VLD001', 'UNOS_ID/Name',
        'Required field is NULL or empty',
        SourceRowNumber
    FROM stg.Recipients
    WHERE LoadID = @LoadID
      AND (UNOS_ID IS NULL OR LTRIM(RTRIM(UNOS_ID)) = ''
           OR FirstName IS NULL OR LTRIM(RTRIM(FirstName)) = ''
           OR LastName IS NULL OR LTRIM(RTRIM(LastName)) = '');
    
    SET @ErrorCount = @ErrorCount + @@ROWCOUNT;
    
    -- VLD002: Blood Type Validation
    INSERT INTO error.Recipients (
        LoadID, UNOS_ID, FirstName, LastName, DateOfBirth, BloodType,
        NeededOrgan, ListingDate, OPO_Code, Status, UrgencyCode, Diagnosis,
        Height_cm, Weight_kg, PRA_Percent,
        ErrorCode, ErrorColumn, ErrorDescription, SourceRowNumber
    )
    SELECT 
        @LoadID, UNOS_ID, FirstName, LastName, DateOfBirth, BloodType,
        NeededOrgan, ListingDate, OPO_Code, Status, UrgencyCode, Diagnosis,
        Height_cm, Weight_kg, PRA_Percent,
        'VLD002', 'BloodType',
        'Invalid blood type: ''' + COALESCE(BloodType, 'NULL') + '''',
        SourceRowNumber
    FROM stg.Recipients s
    WHERE LoadID = @LoadID
      AND UNOS_ID NOT IN (SELECT UNOS_ID FROM error.Recipients WHERE LoadID = @LoadID)
      AND NOT EXISTS (
          SELECT 1 FROM dbo.BloodTypes bt 
          WHERE bt.BloodTypeCode = s.BloodType AND bt.IsActive = 1
      );
    
    SET @ErrorCount = @ErrorCount + @@ROWCOUNT;
    
    -- VLD003: Date Validation (DOB and ListingDate)
    INSERT INTO error.Recipients (
        LoadID, UNOS_ID, FirstName, LastName, DateOfBirth, BloodType,
        NeededOrgan, ListingDate, OPO_Code, Status, UrgencyCode, Diagnosis,
        Height_cm, Weight_kg, PRA_Percent,
        ErrorCode, ErrorColumn, ErrorDescription, SourceRowNumber
    )
    SELECT 
        @LoadID, UNOS_ID, FirstName, LastName, DateOfBirth, BloodType,
        NeededOrgan, ListingDate, OPO_Code, Status, UrgencyCode, Diagnosis,
        Height_cm, Weight_kg, PRA_Percent,
        'VLD003', 'DateOfBirth/ListingDate',
        'Invalid date: DOB=' + COALESCE(DateOfBirth, 'NULL') + ', ListingDate=' + COALESCE(ListingDate, 'NULL'),
        SourceRowNumber
    FROM stg.Recipients s
    WHERE LoadID = @LoadID
      AND UNOS_ID NOT IN (SELECT UNOS_ID FROM error.Recipients WHERE LoadID = @LoadID)
      AND (TRY_CAST(DateOfBirth AS DATE) IS NULL
           OR TRY_CAST(ListingDate AS DATE) IS NULL
           OR TRY_CAST(ListingDate AS DATE) > GETDATE());
    
    SET @ErrorCount = @ErrorCount + @@ROWCOUNT;
    
    -- VLD004: OPO Code Referential Integrity
    INSERT INTO error.Recipients (
        LoadID, UNOS_ID, FirstName, LastName, DateOfBirth, BloodType,
        NeededOrgan, ListingDate, OPO_Code, Status, UrgencyCode, Diagnosis,
        Height_cm, Weight_kg, PRA_Percent,
        ErrorCode, ErrorColumn, ErrorDescription, SourceRowNumber
    )
    SELECT 
        @LoadID, UNOS_ID, FirstName, LastName, DateOfBirth, BloodType,
        NeededOrgan, ListingDate, OPO_Code, Status, UrgencyCode, Diagnosis,
        Height_cm, Weight_kg, PRA_Percent,
        'VLD004', 'OPO_Code',
        'OPO code not found: ''' + COALESCE(OPO_Code, 'NULL') + '''',
        SourceRowNumber
    FROM stg.Recipients s
    WHERE LoadID = @LoadID
      AND UNOS_ID NOT IN (SELECT UNOS_ID FROM error.Recipients WHERE LoadID = @LoadID)
      AND NOT EXISTS (
          SELECT 1 FROM dbo.TransplantCenters tc 
          WHERE tc.OPO_Code = s.OPO_Code AND tc.IsActive = 1
      );
    
    SET @ErrorCount = @ErrorCount + @@ROWCOUNT;
    
    SELECT @ErrorCount AS TotalValidationErrors;
    PRINT 'Recipient validation complete. Errors found: ' + CAST(@ErrorCount AS VARCHAR(10));
END
GO

-- ============================================================================
-- Procedure: Load Valid Donors to Production
-- Uses MERGE pattern for idempotent upsert
-- ============================================================================
IF OBJECT_ID('dbo.usp_LoadValidDonors', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_LoadValidDonors;
GO

CREATE PROCEDURE dbo.usp_LoadValidDonors
    @LoadID INT
AS
BEGIN
    SET NOCOUNT ON;
    
    MERGE dbo.Donors AS target
    USING (
        SELECT 
            s.UNOS_ID,
            s.FirstName,
            s.LastName,
            CAST(s.DateOfBirth AS DATE) AS DateOfBirth,
            DATEDIFF(YEAR, CAST(s.DateOfBirth AS DATE), GETDATE()) AS Age,
            s.BloodType,
            s.OrganType,
            CAST(s.ReferralDate AS DATE) AS ReferralDate,
            tc.CenterID,
            s.DonorType,
            s.Status,
            s.CauseOfDeath,
            TRY_CAST(s.Height_cm AS DECIMAL(5,1)) AS Height_cm,
            TRY_CAST(s.Weight_kg AS DECIMAL(5,1)) AS Weight_kg,
            CASE 
                WHEN TRY_CAST(s.Height_cm AS DECIMAL(5,1)) > 0 
                THEN TRY_CAST(s.Weight_kg AS DECIMAL(5,1)) / 
                     POWER(TRY_CAST(s.Height_cm AS DECIMAL(5,1)) / 100, 2)
                ELSE NULL 
            END AS BMI,
            s.ContactPhone,
            @LoadID AS LoadID
        FROM stg.Donors s
        INNER JOIN dbo.TransplantCenters tc ON s.OPO_Code = tc.OPO_Code
        WHERE s.LoadID = @LoadID
          AND s.UNOS_ID NOT IN (SELECT UNOS_ID FROM error.Donors WHERE LoadID = @LoadID)
    ) AS source
    ON target.UNOS_ID = source.UNOS_ID
    
    WHEN MATCHED THEN
        UPDATE SET
            FirstName = source.FirstName,
            LastName = source.LastName,
            DateOfBirth = source.DateOfBirth,
            Age = source.Age,
            BloodType = source.BloodType,
            OrganType = source.OrganType,
            ReferralDate = source.ReferralDate,
            CenterID = source.CenterID,
            DonorType = source.DonorType,
            Status = source.Status,
            CauseOfDeath = source.CauseOfDeath,
            Height_cm = source.Height_cm,
            Weight_kg = source.Weight_kg,
            BMI = source.BMI,
            ContactPhone = source.ContactPhone,
            ModifiedDate = GETDATE(),
            LoadID = source.LoadID
    
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (UNOS_ID, FirstName, LastName, DateOfBirth, Age, BloodType, 
                OrganType, ReferralDate, CenterID, DonorType, Status, 
                CauseOfDeath, Height_cm, Weight_kg, BMI, ContactPhone, LoadID)
        VALUES (source.UNOS_ID, source.FirstName, source.LastName, source.DateOfBirth,
                source.Age, source.BloodType, source.OrganType, source.ReferralDate,
                source.CenterID, source.DonorType, source.Status, source.CauseOfDeath,
                source.Height_cm, source.Weight_kg, source.BMI, source.ContactPhone, 
                source.LoadID);
    
    PRINT 'Donor production load complete. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
END
GO

-- ============================================================================
-- Procedure: Load Valid Recipients to Production
-- ============================================================================
IF OBJECT_ID('dbo.usp_LoadValidRecipients', 'P') IS NOT NULL DROP PROCEDURE dbo.usp_LoadValidRecipients;
GO

CREATE PROCEDURE dbo.usp_LoadValidRecipients
    @LoadID INT
AS
BEGIN
    SET NOCOUNT ON;
    
    MERGE dbo.Recipients AS target
    USING (
        SELECT 
            s.UNOS_ID,
            s.FirstName,
            s.LastName,
            CAST(s.DateOfBirth AS DATE) AS DateOfBirth,
            DATEDIFF(YEAR, CAST(s.DateOfBirth AS DATE), GETDATE()) AS Age,
            s.BloodType,
            s.NeededOrgan,
            CAST(s.ListingDate AS DATE) AS ListingDate,
            DATEDIFF(DAY, CAST(s.ListingDate AS DATE), GETDATE()) AS DaysOnWaitlist,
            tc.CenterID,
            s.Status,
            s.UrgencyCode,
            s.Diagnosis,
            TRY_CAST(s.Height_cm AS DECIMAL(5,1)) AS Height_cm,
            TRY_CAST(s.Weight_kg AS DECIMAL(5,1)) AS Weight_kg,
            CASE 
                WHEN TRY_CAST(s.Height_cm AS DECIMAL(5,1)) > 0 
                THEN TRY_CAST(s.Weight_kg AS DECIMAL(5,1)) / 
                     POWER(TRY_CAST(s.Height_cm AS DECIMAL(5,1)) / 100, 2)
                ELSE NULL 
            END AS BMI,
            TRY_CAST(s.PRA_Percent AS DECIMAL(5,2)) AS PRA_Percent,
            @LoadID AS LoadID
        FROM stg.Recipients s
        INNER JOIN dbo.TransplantCenters tc ON s.OPO_Code = tc.OPO_Code
        WHERE s.LoadID = @LoadID
          AND s.UNOS_ID NOT IN (SELECT UNOS_ID FROM error.Recipients WHERE LoadID = @LoadID)
    ) AS source
    ON target.UNOS_ID = source.UNOS_ID
    
    WHEN MATCHED THEN
        UPDATE SET
            FirstName = source.FirstName,
            LastName = source.LastName,
            DateOfBirth = source.DateOfBirth,
            Age = source.Age,
            BloodType = source.BloodType,
            NeededOrgan = source.NeededOrgan,
            ListingDate = source.ListingDate,
            DaysOnWaitlist = source.DaysOnWaitlist,
            CenterID = source.CenterID,
            Status = source.Status,
            UrgencyCode = source.UrgencyCode,
            Diagnosis = source.Diagnosis,
            Height_cm = source.Height_cm,
            Weight_kg = source.Weight_kg,
            BMI = source.BMI,
            PRA_Percent = source.PRA_Percent,
            ModifiedDate = GETDATE(),
            LoadID = source.LoadID
    
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (UNOS_ID, FirstName, LastName, DateOfBirth, Age, BloodType,
                NeededOrgan, ListingDate, DaysOnWaitlist, CenterID, Status,
                UrgencyCode, Diagnosis, Height_cm, Weight_kg, BMI, PRA_Percent, LoadID)
        VALUES (source.UNOS_ID, source.FirstName, source.LastName, source.DateOfBirth,
                source.Age, source.BloodType, source.NeededOrgan, source.ListingDate,
                source.DaysOnWaitlist, source.CenterID, source.Status, source.UrgencyCode,
                source.Diagnosis, source.Height_cm, source.Weight_kg, source.BMI,
                source.PRA_Percent, source.LoadID);
    
    PRINT 'Recipient production load complete. Rows affected: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
END
GO

PRINT 'Validation and load procedures created successfully.';
GO
