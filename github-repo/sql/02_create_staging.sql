/*
================================================================================
Organ Donation ETL Pipeline - Staging Tables
================================================================================
Script: 02_create_staging.sql
Purpose: Creates staging tables for initial CSV data landing
Author: [Your Name]
Date: 2025

Design Rationale:
- All columns are VARCHAR to accept any source data without type conversion failure
- This allows validation to happen AFTER data is safely staged
- Original source data is preserved for audit and troubleshooting
- ETL metadata columns track lineage (LoadID, SourceFileName, RowNumber)
================================================================================
*/

USE OrganDonationDB;
GO

-- Create staging schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg');
GO

-- ============================================================================
-- Staging: Donors
-- Source: donors.csv
-- ============================================================================
IF OBJECT_ID('stg.Donors', 'U') IS NOT NULL DROP TABLE stg.Donors;
GO

CREATE TABLE stg.Donors
(
    -- Source data columns (all VARCHAR to accept any input)
    UNOS_ID             VARCHAR(50),
    FirstName           VARCHAR(100),
    LastName            VARCHAR(100),
    DateOfBirth         VARCHAR(50),
    BloodType           VARCHAR(20),
    OrganType           VARCHAR(50),
    ReferralDate        VARCHAR(50),
    OPO_Code            VARCHAR(20),
    DonorType           VARCHAR(20),       -- DBD (Brain Death) or DCD (Cardiac Death)
    Status              VARCHAR(50),
    CauseOfDeath        VARCHAR(200),
    Height_cm           VARCHAR(20),
    Weight_kg           VARCHAR(20),
    ContactPhone        VARCHAR(50),
    
    -- ETL metadata columns (added during load)
    LoadID              INT,               -- Foreign key to audit.ETL_LoadLog
    SourceFileName      VARCHAR(255),
    LoadDateTime        DATETIME DEFAULT GETDATE(),
    SourceRowNumber     INT
);
GO

-- ============================================================================
-- Staging: Recipients
-- Source: recipients.csv
-- ============================================================================
IF OBJECT_ID('stg.Recipients', 'U') IS NOT NULL DROP TABLE stg.Recipients;
GO

CREATE TABLE stg.Recipients
(
    UNOS_ID             VARCHAR(50),
    FirstName           VARCHAR(100),
    LastName            VARCHAR(100),
    DateOfBirth         VARCHAR(50),
    BloodType           VARCHAR(20),
    NeededOrgan         VARCHAR(50),
    ListingDate         VARCHAR(50),
    OPO_Code            VARCHAR(20),
    Status              VARCHAR(20),
    UrgencyCode         VARCHAR(20),       -- UNOS urgency status (1A, 1B, 2, 3)
    Diagnosis           VARCHAR(200),
    Height_cm           VARCHAR(20),
    Weight_kg           VARCHAR(20),
    PRA_Percent         VARCHAR(20),       -- Panel Reactive Antibody percentage
    
    -- ETL metadata
    LoadID              INT,
    SourceFileName      VARCHAR(255),
    LoadDateTime        DATETIME DEFAULT GETDATE(),
    SourceRowNumber     INT
);
GO

-- ============================================================================
-- Staging: Transplant Centers
-- Source: transplant_centers.csv
-- ============================================================================
IF OBJECT_ID('stg.TransplantCenters', 'U') IS NOT NULL DROP TABLE stg.TransplantCenters;
GO

CREATE TABLE stg.TransplantCenters
(
    OPO_Code            VARCHAR(20),
    OPO_Name            VARCHAR(200),
    City                VARCHAR(100),
    State               VARCHAR(50),
    Region              VARCHAR(20),       -- UNOS region (1-11)
    CenterType          VARCHAR(100),
    CMS_Certification   VARCHAR(50),
    AccreditationDate   VARCHAR(50),
    Phone               VARCHAR(50),
    Email               VARCHAR(200),
    
    -- ETL metadata
    LoadID              INT,
    SourceFileName      VARCHAR(255),
    LoadDateTime        DATETIME DEFAULT GETDATE(),
    SourceRowNumber     INT
);
GO

PRINT 'Staging tables created successfully in [stg] schema.';
PRINT 'Tables: stg.Donors, stg.Recipients, stg.TransplantCenters';
GO
