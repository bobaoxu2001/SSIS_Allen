/*
================================================================================
Organ Donation ETL Pipeline - Database Creation
================================================================================
Script: 01_create_database.sql
Purpose: Creates the OrganDonationDB database with appropriate settings
Author: Allen Xu
Date: 2025

Notes:
- SQL Server Developer Edition recommended (free, full-featured)
- Adjust file paths as needed for your environment
- FULL recovery model supports point-in-time recovery (compliance requirement)
================================================================================
*/

USE master;
GO

-- Drop existing database if present (development only)
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'OrganDonationDB')
BEGIN
    ALTER DATABASE OrganDonationDB SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE OrganDonationDB;
END
GO

-- Create database with explicit file configuration
-- Adjust paths based on your SQL Server installation
CREATE DATABASE OrganDonationDB;
GO

USE OrganDonationDB;
GO

-- Set recovery model to FULL for compliance (point-in-time recovery)
ALTER DATABASE OrganDonationDB SET RECOVERY FULL;
GO

-- Enable snapshot isolation for better concurrency during ETL
ALTER DATABASE OrganDonationDB SET ALLOW_SNAPSHOT_ISOLATION ON;
GO

PRINT 'Database OrganDonationDB created successfully.';
PRINT 'Recovery Model: FULL';
PRINT 'Snapshot Isolation: Enabled';
GO
