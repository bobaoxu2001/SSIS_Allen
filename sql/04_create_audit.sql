/*
================================================================================
Organ Donation ETL Pipeline - Audit and Error Tables
================================================================================
Script: 04_create_audit.sql
Purpose: Creates audit logging and error capture infrastructure
Author: Allen Xu
Date: 2025

Design Rationale:
- Audit tables support compliance requirements (HIPAA, CMS regulations)
- Error tables preserve original data + specific failure reason
- Enables reconciliation: source count = staged + errors
- Supports troubleshooting and data quality improvement
================================================================================
*/

USE OrganDonationDB;
GO

-- Create schemas for audit and error tables
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'audit')
    EXEC('CREATE SCHEMA audit');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'error')
    EXEC('CREATE SCHEMA error');
GO

-- ============================================================================
-- Audit: ETL Load Log
-- Master table tracking all ETL executions
-- ============================================================================
IF OBJECT_ID('audit.ETL_LoadLog', 'U') IS NOT NULL DROP TABLE audit.ETL_LoadLog;
GO

CREATE TABLE audit.ETL_LoadLog
(
    LoadID              INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    PackageName         VARCHAR(255) NOT NULL,
    ExecutionStartTime  DATETIME NOT NULL DEFAULT GETDATE(),
    ExecutionEndTime    DATETIME NULL,
    Status              VARCHAR(50) NOT NULL DEFAULT 'Running',
    SourceFileName      VARCHAR(255) NULL,
    
    -- Row count metrics for reconciliation
    SourceRowCount      INT NULL,
    StagingRowCount     INT NULL,
    InsertedRowCount    INT NULL,
    UpdatedRowCount     INT NULL,
    ErrorRowCount       INT NULL,
    
    -- Execution context
    ExecutedBy          VARCHAR(128) DEFAULT SYSTEM_USER,
    MachineName         VARCHAR(128) DEFAULT HOST_NAME(),
    ErrorMessage        VARCHAR(MAX) NULL,
    
    CONSTRAINT CK_ETLLoadLog_Status CHECK (Status IN ('Running', 'Success', 'Failed', 'Warning'))
);
GO

CREATE NONCLUSTERED INDEX IX_ETLLoadLog_StartTime ON audit.ETL_LoadLog(ExecutionStartTime DESC);
CREATE NONCLUSTERED INDEX IX_ETLLoadLog_Status ON audit.ETL_LoadLog(Status);
CREATE NONCLUSTERED INDEX IX_ETLLoadLog_PackageName ON audit.ETL_LoadLog(PackageName);
GO

-- ============================================================================
-- Error: Donor Error Records
-- Captures donor records that failed validation
-- ============================================================================
IF OBJECT_ID('error.Donors', 'U') IS NOT NULL DROP TABLE error.Donors;
GO

CREATE TABLE error.Donors
(
    ErrorID             INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    LoadID              INT NOT NULL,
    
    -- Original source data preserved exactly as received
    UNOS_ID             VARCHAR(50),
    FirstName           VARCHAR(100),
    LastName            VARCHAR(100),
    DateOfBirth         VARCHAR(50),
    BloodType           VARCHAR(20),
    OrganType           VARCHAR(50),
    ReferralDate        VARCHAR(50),
    OPO_Code            VARCHAR(20),
    DonorType           VARCHAR(20),
    Status              VARCHAR(50),
    CauseOfDeath        VARCHAR(200),
    Height_cm           VARCHAR(20),
    Weight_kg           VARCHAR(20),
    ContactPhone        VARCHAR(50),
    
    -- Error details
    ErrorCode           VARCHAR(20) NOT NULL,
    ErrorColumn         VARCHAR(100) NULL,
    ErrorDescription    VARCHAR(500) NOT NULL,
    ErrorDateTime       DATETIME NOT NULL DEFAULT GETDATE(),
    SourceRowNumber     INT NULL,
    
    CONSTRAINT FK_ErrorDonors_LoadLog FOREIGN KEY (LoadID) 
        REFERENCES audit.ETL_LoadLog(LoadID)
);
GO

CREATE NONCLUSTERED INDEX IX_ErrorDonors_LoadID ON error.Donors(LoadID);
CREATE NONCLUSTERED INDEX IX_ErrorDonors_ErrorCode ON error.Donors(ErrorCode);
GO

-- ============================================================================
-- Error: Recipient Error Records
-- Captures recipient records that failed validation
-- ============================================================================
IF OBJECT_ID('error.Recipients', 'U') IS NOT NULL DROP TABLE error.Recipients;
GO

CREATE TABLE error.Recipients
(
    ErrorID             INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    LoadID              INT NOT NULL,
    
    -- Original source data
    UNOS_ID             VARCHAR(50),
    FirstName           VARCHAR(100),
    LastName            VARCHAR(100),
    DateOfBirth         VARCHAR(50),
    BloodType           VARCHAR(20),
    NeededOrgan         VARCHAR(50),
    ListingDate         VARCHAR(50),
    OPO_Code            VARCHAR(20),
    Status              VARCHAR(20),
    UrgencyCode         VARCHAR(20),
    Diagnosis           VARCHAR(200),
    Height_cm           VARCHAR(20),
    Weight_kg           VARCHAR(20),
    PRA_Percent         VARCHAR(20),
    
    -- Error details
    ErrorCode           VARCHAR(20) NOT NULL,
    ErrorColumn         VARCHAR(100) NULL,
    ErrorDescription    VARCHAR(500) NOT NULL,
    ErrorDateTime       DATETIME NOT NULL DEFAULT GETDATE(),
    SourceRowNumber     INT NULL,
    
    CONSTRAINT FK_ErrorRecipients_LoadLog FOREIGN KEY (LoadID) 
        REFERENCES audit.ETL_LoadLog(LoadID)
);
GO

CREATE NONCLUSTERED INDEX IX_ErrorRecipients_LoadID ON error.Recipients(LoadID);
CREATE NONCLUSTERED INDEX IX_ErrorRecipients_ErrorCode ON error.Recipients(ErrorCode);
GO

-- ============================================================================
-- Stored Procedure: Start ETL Load
-- Call at the beginning of each package execution
-- ============================================================================
IF OBJECT_ID('audit.usp_StartETLLoad', 'P') IS NOT NULL DROP PROCEDURE audit.usp_StartETLLoad;
GO

CREATE PROCEDURE audit.usp_StartETLLoad
    @PackageName VARCHAR(255),
    @SourceFileName VARCHAR(255) = NULL,
    @LoadID INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    INSERT INTO audit.ETL_LoadLog (PackageName, SourceFileName, Status)
    VALUES (@PackageName, @SourceFileName, 'Running');
    
    SET @LoadID = SCOPE_IDENTITY();
    
    PRINT 'ETL Load started. LoadID: ' + CAST(@LoadID AS VARCHAR(10));
END
GO

-- ============================================================================
-- Stored Procedure: Complete ETL Load
-- Call at the end of each package execution
-- ============================================================================
IF OBJECT_ID('audit.usp_CompleteETLLoad', 'P') IS NOT NULL DROP PROCEDURE audit.usp_CompleteETLLoad;
GO

CREATE PROCEDURE audit.usp_CompleteETLLoad
    @LoadID INT,
    @Status VARCHAR(50),
    @SourceRowCount INT = NULL,
    @StagingRowCount INT = NULL,
    @InsertedRowCount INT = NULL,
    @UpdatedRowCount INT = NULL,
    @ErrorRowCount INT = NULL,
    @ErrorMessage VARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    UPDATE audit.ETL_LoadLog
    SET 
        ExecutionEndTime = GETDATE(),
        Status = @Status,
        SourceRowCount = @SourceRowCount,
        StagingRowCount = @StagingRowCount,
        InsertedRowCount = @InsertedRowCount,
        UpdatedRowCount = @UpdatedRowCount,
        ErrorRowCount = @ErrorRowCount,
        ErrorMessage = @ErrorMessage
    WHERE LoadID = @LoadID;
    
    -- Calculate duration for logging
    DECLARE @Duration INT;
    SELECT @Duration = DATEDIFF(SECOND, ExecutionStartTime, ExecutionEndTime)
    FROM audit.ETL_LoadLog WHERE LoadID = @LoadID;
    
    PRINT 'ETL Load completed. Status: ' + @Status + ', Duration: ' + CAST(@Duration AS VARCHAR(10)) + ' seconds';
END
GO

PRINT 'Audit and error infrastructure created successfully.';
PRINT 'Schemas: [audit], [error]';
PRINT 'Tables: audit.ETL_LoadLog, error.Donors, error.Recipients';
PRINT 'Procedures: audit.usp_StartETLLoad, audit.usp_CompleteETLLoad';
GO
