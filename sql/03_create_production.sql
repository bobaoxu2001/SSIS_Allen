/*
================================================================================
Organ Donation ETL Pipeline - Production Tables
================================================================================
Script: 03_create_production.sql
Purpose: Creates production tables with proper data types and constraints
Author: Allen Xu
Date: 2025

Design Rationale:
- Proper data types enforce data quality at the database level
- Foreign keys ensure referential integrity
- Check constraints limit values to valid domains
- Indexes support common query patterns
- Audit columns (CreatedDate, ModifiedDate) track record history
================================================================================
*/

USE OrganDonationDB;
GO

-- ============================================================================
-- Reference Table: Blood Types
-- Valid ABO/Rh combinations per ISBT 128 standard
-- ============================================================================
IF OBJECT_ID('dbo.BloodTypes', 'U') IS NOT NULL DROP TABLE dbo.BloodTypes;
GO

CREATE TABLE dbo.BloodTypes
(
    BloodTypeCode   VARCHAR(5) NOT NULL PRIMARY KEY,
    BloodTypeName   VARCHAR(50) NOT NULL,
    ABO_Group       CHAR(2) NOT NULL,
    Rh_Factor       CHAR(1) NOT NULL,
    IsActive        BIT NOT NULL DEFAULT 1
);
GO

INSERT INTO dbo.BloodTypes (BloodTypeCode, BloodTypeName, ABO_Group, Rh_Factor) VALUES
('O+', 'O Positive', 'O', '+'),
('O-', 'O Negative', 'O', '-'),
('A+', 'A Positive', 'A', '+'),
('A-', 'A Negative', 'A', '-'),
('B+', 'B Positive', 'B', '+'),
('B-', 'B Negative', 'B', '-'),
('AB+', 'AB Positive', 'AB', '+'),
('AB-', 'AB Negative', 'AB', '-');
GO

-- ============================================================================
-- Reference Table: Organ Types
-- Based on OPTN organ classification
-- ============================================================================
IF OBJECT_ID('dbo.OrganTypes', 'U') IS NOT NULL DROP TABLE dbo.OrganTypes;
GO

CREATE TABLE dbo.OrganTypes
(
    OrganTypeCode   VARCHAR(20) NOT NULL PRIMARY KEY,
    OrganTypeName   VARCHAR(100) NOT NULL,
    OrganCategory   VARCHAR(50) NOT NULL,  -- Solid Organ, Tissue, etc.
    IsActive        BIT NOT NULL DEFAULT 1
);
GO

INSERT INTO dbo.OrganTypes (OrganTypeCode, OrganTypeName, OrganCategory) VALUES
('Kidney', 'Kidney', 'Solid Organ'),
('Liver', 'Liver', 'Solid Organ'),
('Heart', 'Heart', 'Solid Organ'),
('Lung', 'Lung', 'Solid Organ'),
('Pancreas', 'Pancreas', 'Solid Organ'),
('Intestine', 'Intestine', 'Solid Organ'),
('Heart-Lung', 'Heart-Lung', 'Combined'),
('Kidney-Pancreas', 'Kidney-Pancreas', 'Combined');
GO

-- ============================================================================
-- Reference Table: UNOS Status Codes
-- Waitlist status codes per OPTN policy
-- ============================================================================
IF OBJECT_ID('dbo.StatusCodes', 'U') IS NOT NULL DROP TABLE dbo.StatusCodes;
GO

CREATE TABLE dbo.StatusCodes
(
    StatusCode      VARCHAR(10) NOT NULL PRIMARY KEY,
    StatusName      VARCHAR(100) NOT NULL,
    StatusCategory  VARCHAR(50) NOT NULL,
    IsActive        BIT NOT NULL DEFAULT 1
);
GO

INSERT INTO dbo.StatusCodes (StatusCode, StatusName, StatusCategory) VALUES
('1', 'Active - Status 1', 'Active'),
('1A', 'Status 1A - Highest Urgency', 'Active'),
('1B', 'Status 1B - High Urgency', 'Active'),
('2', 'Status 2 - Moderate Urgency', 'Active'),
('3', 'Status 3 - Standard', 'Active'),
('7', 'Inactive - Temporarily Unsuitable', 'Inactive'),
('Active', 'Active Donor', 'Active'),
('Inactive', 'Inactive Donor', 'Inactive'),
('Pending', 'Pending Evaluation', 'Pending');
GO

-- ============================================================================
-- Dimension Table: Transplant Centers
-- OPO and transplant program information
-- ============================================================================
IF OBJECT_ID('dbo.TransplantCenters', 'U') IS NOT NULL DROP TABLE dbo.TransplantCenters;
GO

CREATE TABLE dbo.TransplantCenters
(
    CenterID            INT IDENTITY(1,1) NOT NULL,
    OPO_Code            VARCHAR(20) NOT NULL,
    OPO_Name            VARCHAR(200) NOT NULL,
    City                VARCHAR(100) NOT NULL,
    State               CHAR(2) NOT NULL,
    Region              TINYINT NOT NULL,
    CenterType          VARCHAR(100) NOT NULL,
    CMS_Certification   VARCHAR(50) NULL,
    AccreditationDate   DATE NULL,
    Phone               VARCHAR(50) NULL,
    Email               VARCHAR(200) NULL,
    
    -- Audit columns
    CreatedDate         DATETIME NOT NULL DEFAULT GETDATE(),
    ModifiedDate        DATETIME NOT NULL DEFAULT GETDATE(),
    IsActive            BIT NOT NULL DEFAULT 1,
    
    CONSTRAINT PK_TransplantCenters PRIMARY KEY (CenterID),
    CONSTRAINT UQ_TransplantCenters_OPOCode UNIQUE (OPO_Code),
    CONSTRAINT CK_TransplantCenters_Region CHECK (Region BETWEEN 1 AND 11)
);
GO

CREATE NONCLUSTERED INDEX IX_TransplantCenters_OPOCode ON dbo.TransplantCenters(OPO_Code);
CREATE NONCLUSTERED INDEX IX_TransplantCenters_State ON dbo.TransplantCenters(State);
GO

-- ============================================================================
-- Fact Table: Donors
-- Organ donor registrations and referrals
-- ============================================================================
IF OBJECT_ID('dbo.Donors', 'U') IS NOT NULL DROP TABLE dbo.Donors;
GO

CREATE TABLE dbo.Donors
(
    DonorID             INT IDENTITY(1,1) NOT NULL,
    UNOS_ID             VARCHAR(50) NOT NULL,
    FirstName           VARCHAR(100) NOT NULL,
    LastName            VARCHAR(100) NOT NULL,
    DateOfBirth         DATE NOT NULL,
    Age                 INT NULL,
    BloodType           VARCHAR(5) NOT NULL,
    OrganType           VARCHAR(20) NOT NULL,
    ReferralDate        DATE NOT NULL,
    CenterID            INT NOT NULL,
    DonorType           VARCHAR(10) NOT NULL,  -- DBD or DCD
    Status              VARCHAR(20) NOT NULL,
    CauseOfDeath        VARCHAR(200) NULL,
    Height_cm           DECIMAL(5,1) NULL,
    Weight_kg           DECIMAL(5,1) NULL,
    BMI                 DECIMAL(4,1) NULL,
    ContactPhone        VARCHAR(50) NULL,
    
    -- Audit columns
    CreatedDate         DATETIME NOT NULL DEFAULT GETDATE(),
    ModifiedDate        DATETIME NOT NULL DEFAULT GETDATE(),
    LoadID              INT NULL,
    
    CONSTRAINT PK_Donors PRIMARY KEY (DonorID),
    CONSTRAINT UQ_Donors_UNOSID UNIQUE (UNOS_ID),
    CONSTRAINT FK_Donors_Center FOREIGN KEY (CenterID) 
        REFERENCES dbo.TransplantCenters(CenterID),
    CONSTRAINT FK_Donors_BloodType FOREIGN KEY (BloodType) 
        REFERENCES dbo.BloodTypes(BloodTypeCode),
    CONSTRAINT FK_Donors_OrganType FOREIGN KEY (OrganType) 
        REFERENCES dbo.OrganTypes(OrganTypeCode),
    CONSTRAINT CK_Donors_DonorType CHECK (DonorType IN ('DBD', 'DCD')),
    CONSTRAINT CK_Donors_Age CHECK (Age IS NULL OR (Age >= 0 AND Age <= 120))
);
GO

CREATE NONCLUSTERED INDEX IX_Donors_BloodType ON dbo.Donors(BloodType);
CREATE NONCLUSTERED INDEX IX_Donors_OrganType ON dbo.Donors(OrganType);
CREATE NONCLUSTERED INDEX IX_Donors_Status ON dbo.Donors(Status) WHERE Status = 'Active';
CREATE NONCLUSTERED INDEX IX_Donors_CenterID ON dbo.Donors(CenterID);
CREATE NONCLUSTERED INDEX IX_Donors_ReferralDate ON dbo.Donors(ReferralDate);
GO

-- ============================================================================
-- Fact Table: Recipients
-- Patients on transplant waitlist
-- ============================================================================
IF OBJECT_ID('dbo.Recipients', 'U') IS NOT NULL DROP TABLE dbo.Recipients;
GO

CREATE TABLE dbo.Recipients
(
    RecipientID         INT IDENTITY(1,1) NOT NULL,
    UNOS_ID             VARCHAR(50) NOT NULL,
    FirstName           VARCHAR(100) NOT NULL,
    LastName            VARCHAR(100) NOT NULL,
    DateOfBirth         DATE NOT NULL,
    Age                 INT NULL,
    BloodType           VARCHAR(5) NOT NULL,
    NeededOrgan         VARCHAR(20) NOT NULL,
    ListingDate         DATE NOT NULL,
    DaysOnWaitlist      INT NULL,
    CenterID            INT NOT NULL,
    Status              VARCHAR(20) NOT NULL,
    UrgencyCode         VARCHAR(10) NOT NULL,
    Diagnosis           VARCHAR(200) NULL,
    Height_cm           DECIMAL(5,1) NULL,
    Weight_kg           DECIMAL(5,1) NULL,
    BMI                 DECIMAL(4,1) NULL,
    PRA_Percent         DECIMAL(5,2) NULL,  -- Panel Reactive Antibody
    
    -- Audit columns
    CreatedDate         DATETIME NOT NULL DEFAULT GETDATE(),
    ModifiedDate        DATETIME NOT NULL DEFAULT GETDATE(),
    LoadID              INT NULL,
    
    CONSTRAINT PK_Recipients PRIMARY KEY (RecipientID),
    CONSTRAINT UQ_Recipients_UNOSID UNIQUE (UNOS_ID),
    CONSTRAINT FK_Recipients_Center FOREIGN KEY (CenterID) 
        REFERENCES dbo.TransplantCenters(CenterID),
    CONSTRAINT FK_Recipients_BloodType FOREIGN KEY (BloodType) 
        REFERENCES dbo.BloodTypes(BloodTypeCode),
    CONSTRAINT FK_Recipients_OrganType FOREIGN KEY (NeededOrgan) 
        REFERENCES dbo.OrganTypes(OrganTypeCode),
    CONSTRAINT CK_Recipients_Age CHECK (Age IS NULL OR (Age >= 0 AND Age <= 120)),
    CONSTRAINT CK_Recipients_PRA CHECK (PRA_Percent IS NULL OR (PRA_Percent >= 0 AND PRA_Percent <= 100))
);
GO

CREATE NONCLUSTERED INDEX IX_Recipients_BloodType ON dbo.Recipients(BloodType);
CREATE NONCLUSTERED INDEX IX_Recipients_NeededOrgan ON dbo.Recipients(NeededOrgan);
CREATE NONCLUSTERED INDEX IX_Recipients_UrgencyCode ON dbo.Recipients(UrgencyCode);
CREATE NONCLUSTERED INDEX IX_Recipients_ListingDate ON dbo.Recipients(ListingDate);
CREATE NONCLUSTERED INDEX IX_Recipients_CenterID ON dbo.Recipients(CenterID);
GO

PRINT 'Production tables created successfully.';
PRINT 'Reference tables: BloodTypes, OrganTypes, StatusCodes, TransplantCenters';
PRINT 'Fact tables: Donors, Recipients';
GO
