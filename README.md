# Organ Donation ETL Pipeline

**Author**: Allen Xu  
**Date**: January 2025  
**Tech Stack**: SQL Server 2022 | SSIS | T-SQL

A complete SSIS ETL pipeline demonstrating enterprise data engineering practices for healthcare organ donation and transplantation data processing.

---

## Overview

This project simulates a real-world ETL workflow for an Organ Procurement Organization (OPO), processing donor referrals, recipient waitlist data, and transplant center information. It demonstrates data quality controls, validation, error handling, and audit logging required in regulated healthcare environments.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                           SOURCE DATA                                │
│  donors.csv  │  recipients.csv  │  transplant_centers.csv           │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        SSIS ETL LAYER                                │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐             │
│  │  Master     │───▶│   Load      │───▶│   Load      │             │
│  │  Package    │    │  Centers    │    │  Donors/    │             │
│  │             │    │  (Ref Data) │    │  Recipients │             │
│  └─────────────┘    └─────────────┘    └─────────────┘             │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      SQL SERVER DATABASE                             │
│                                                                      │
│  ┌───────────┐     ┌───────────┐     ┌───────────┐                 │
│  │  STAGING  │────▶│ VALIDATION│────▶│PRODUCTION │                 │
│  │  (stg.*)  │     │  (procs)  │     │  (dbo.*)  │                 │
│  └───────────┘     └─────┬─────┘     └───────────┘                 │
│                          │                                          │
│                    ┌─────▼─────┐     ┌───────────┐                 │
│                    │  ERRORS   │     │   AUDIT   │                 │
│                    │ (error.*) │     │ (audit.*) │                 │
│                    └───────────┘     └───────────┘                 │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Sources

This project uses mock data structured according to real healthcare data standards:

- **OPTN (Organ Procurement and Transplantation Network)** - U.S. national transplant data standards
- **UNOS (United Network for Organ Sharing)** - Organ matching and allocation data formats
- **SRTR (Scientific Registry of Transplant Recipients)** - Waitlist and outcome data structures

Reference: [OPTN Data](https://optn.transplant.hrsa.gov/data/) | [SRTR Reports](https://www.srtr.org/reports/)

## Project Structure

```
organ-donation-etl/
├── data/
│   ├── donors.csv                 # Donor referral records
│   ├── recipients.csv             # Waitlist recipient records
│   └── transplant_centers.csv     # OPO and transplant center reference data
│
├── sql/
│   ├── 01_create_database.sql     # Database initialization
│   ├── 02_create_staging.sql      # Staging tables (all VARCHAR)
│   ├── 03_create_production.sql   # Production tables with constraints
│   ├── 04_create_audit.sql        # Audit and error tables
│   ├── 05_validation_procs.sql    # Data validation stored procedures
│   └── 06_reporting_views.sql     # Reporting queries and views
│
├── ssis/
│   └── package_specifications.md  # SSIS package design specifications
│
├── docs/
│   ├── data_dictionary.md         # Field definitions and valid values
│   ├── validation_rules.md        # Business rules for data quality
│   └── technical_design.md        # Architecture and design decisions
│
└── README.md
```

## Key Features

### 1. Staging Pattern
All source data lands in staging tables with VARCHAR columns to accept any input without failure. This preserves original data for audit and enables validation before production load.

### 2. Multi-Layer Validation
- **Completeness**: Required fields (UNOS_ID, blood type, organ type) must not be null
- **Validity**: Blood type must match ABO/Rh format; dates must be valid and logical
- **Referential Integrity**: OPO codes must exist in transplant center reference table
- **Range Checks**: Age must be 0-120; waitlist time must be positive

### 3. Error Handling
Invalid records are redirected to error tables with:
- Original source data preserved exactly as received
- Specific error code and description
- Source row number for traceability
- Timestamp for audit

### 4. Audit Logging
Every ETL execution logs:
- Package name and execution timestamps
- Source row count, staged count, inserted count, error count
- Success/failure status
- Execution context (user, machine)

## Database Schema

### Reference Tables
| Table | Description |
|-------|-------------|
| `dbo.BloodTypes` | Valid ABO/Rh blood type codes |
| `dbo.OrganTypes` | Transplantable organ categories per OPTN |
| `dbo.StatusCodes` | UNOS waitlist status codes |
| `dbo.TransplantCenters` | OPO and transplant program information |

### Transactional Tables
| Table | Description |
|-------|-------------|
| `dbo.Donors` | Registered organ donors |
| `dbo.Recipients` | Patients on transplant waitlist |

### ETL Infrastructure
| Table | Description |
|-------|-------------|
| `stg.Donors` | Staging table for donor data |
| `stg.Recipients` | Staging table for recipient data |
| `error.Donors` | Failed donor records with error details |
| `error.Recipients` | Failed recipient records with error details |
| `audit.ETL_LoadLog` | ETL execution history |

## Validation Rules

| Rule | Field | Validation |
|------|-------|------------|
| VLD001 | UNOS_ID | Required, unique, format: XXXX-XXXXX |
| VLD002 | BloodType | Must be valid ABO/Rh: A+, A-, B+, B-, AB+, AB-, O+, O- |
| VLD003 | OrganType | Must exist in OrganTypes reference table |
| VLD004 | DateOfBirth | Valid date, age between 0-120 years |
| VLD005 | ListingDate | Valid date, not in future |
| VLD006 | OPO_Code | Must exist in TransplantCenters reference table |
| VLD007 | Status | Must be valid UNOS status code |

## Technology Stack

- **Database**: SQL Server 2019/2022
- **ETL Tool**: SQL Server Integration Services (SSIS)
- **IDE**: Visual Studio 2022 with SSDT
- **Language**: T-SQL

## Getting Started

### Prerequisites
- SQL Server Developer Edition (free)
- SQL Server Management Studio (SSMS)
- Visual Studio 2022 with SSIS extension

### Installation

1. Clone this repository
2. Run SQL scripts in order (01 through 06)
3. Copy CSV files to `C:\Data\` on the server
4. Build SSIS packages following specifications in `ssis/package_specifications.md`
5. Execute the master package

### Quick Test (Without SSIS)
Run `sql/07_manual_etl_test.sql` to simulate the complete ETL flow using pure T-SQL.

## Screenshots

### ETL Execution Results

**Audit Log** - Every ETL execution is tracked:
```
┌────────┬────────────┬─────────┬────────┬──────────┬────────┬──────────┐
│ LoadID │  Package   │ Status  │ Source │ Inserted │ Errors │ Duration │
├────────┼────────────┼─────────┼────────┼──────────┼────────┼──────────┤
│      1 │ MasterETL  │ Success │     64 │       55 │      9 │    12sec │
└────────┴────────────┴─────────┴────────┴──────────┴────────┴──────────┘
```

**Error Capture** - Invalid records captured with details:
```
┌─────────────┬─────────┬─────────────┬───────────────────────────────────────────┐
│   UNOS_ID   │  Code   │   Column    │             ErrorDescription              │
├─────────────┼─────────┼─────────────┼───────────────────────────────────────────┤
│ XXXX-00001  │ VLD003  │ DateOfBirth │ Invalid or missing date of birth: 'NULL' │
│ YYYY-00002  │ VLD002  │ BloodType   │ Invalid blood type: 'X+'. Valid: O±,A±.. │
│ ZZZZ-00003  │ VLD003  │ DateOfBirth │ Date of birth in future: 2030-07-22      │
│ AAAA-00004  │ VLD004  │ OPO_Code    │ OPO code not found: 'INVALID'            │
└─────────────┴─────────┴─────────────┴───────────────────────────────────────────┘
```

**Production Data** - Clean, validated records:
```
┌─────────────┬─────────┬───────────┬───────────┬───────────┬────────┐
│   UNOS_ID   │  Name   │ BloodType │ OrganType │  Status   │  Age   │
├─────────────┼─────────┼───────────┼───────────┼───────────┼────────┤
│ ABJK-78234  │ M.Pat.. │    O+     │  Kidney   │  Active   │   39   │
│ CDMN-45892  │ S.Mit.. │    A-     │   Liver   │  Active   │   34   │
│ GHRS-89123  │ E.Tho.. │   AB+     │  Kidney   │  Active   │   29   │
└─────────────┴─────────┴───────────┴───────────┴───────────┴────────┘
```

### SSIS Package Design

**Control Flow** - ETL orchestration:
```
┌────────────────────┐
│   Start Audit Log  │
└─────────┬──────────┘
          ▼
┌────────────────────┐
│ Truncate Staging   │
└─────────┬──────────┘
          ▼
┌────────────────────┐
│ ┌────────────────┐ │
│ │ Flat File Src  │ │
│ │      ↓         │ │
│ │  Row Count     │ │  ← Data Flow Task
│ │      ↓         │ │
│ │Derived Column  │ │
│ │      ↓         │ │
│ │OLE DB Dest     │ │
│ └────────────────┘ │
└─────────┬──────────┘
          ▼
┌────────────────────┐
│ Validate Data      │  ← Execute SQL Task
│ (Stored Procedure) │
└─────────┬──────────┘
          ▼
┌────────────────────┐
│ Load to Production │  ← Execute SQL Task
│ (MERGE Pattern)    │
└─────────┬──────────┘
          ▼
┌────────────────────┐
│ Complete Audit Log │
└────────────────────┘
```

**Master Package** - Orchestration:
```
              ┌─────────────────────┐
              │   Load Centers      │  ← Reference data FIRST
              │   (Reference)       │
              └──────────┬──────────┘
                         │
           ┌─────────────┴─────────────┐
           ▼                           ▼
┌─────────────────────┐     ┌─────────────────────┐
│    Load Donors      │     │   Load Recipients   │  ← Parallel
└─────────────────────┘     └─────────────────────┘
```

## Design Principles

1. **Never lose data** - Stage everything, validate after
2. **Errors are information** - Capture and analyze, don't discard
3. **Audit everything** - Compliance requires full traceability
4. **Idempotent operations** - Safe to re-run (MERGE pattern)
5. **Separation of concerns** - One package per entity

## Healthcare Compliance Considerations

This design supports regulatory requirements common in healthcare:
- **Data Lineage**: Every record traceable to source file and load batch
- **Audit Trail**: Complete history of data changes
- **Error Accountability**: All rejected records preserved with rejection reason
- **Referential Integrity**: Prevents orphaned or inconsistent records

## License

This project is for educational and demonstration purposes. Sample data is synthetic and does not contain any real patient information.

## Results Summary

| Metric | Value |
|--------|-------|
| Source Records | 64 (35 donors + 29 recipients) |
| Valid Records | 55 (86%) |
| Error Records | 9 (captured with reasons) |
| Reference Tables | 4 (BloodTypes, OrganTypes, StatusCodes, Centers) |
| Stored Procedures | 6 (validation + load) |
| Reporting Views | 5 (executive dashboard, matches, etc.) |

## References

- OPTN Policies: https://optn.transplant.hrsa.gov/policies-bylaws/
- UNOS Data Dictionary: https://unos.org/data/
- SRTR Annual Data Report: https://www.srtr.org/reports/

---

**Author**: Allen Xu | January 2025
