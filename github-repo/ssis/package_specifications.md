# SSIS Package Specifications

This document provides technical specifications for building the SSIS packages.

## Package Overview

| Package | Purpose | Execution Order |
|---------|---------|-----------------|
| `MasterETL.dtsx` | Orchestration - calls child packages | 1 (Entry Point) |
| `LoadTransplantCenters.dtsx` | Loads reference data | 2 (First) |
| `LoadDonors.dtsx` | Loads donor records | 3 (After Centers) |
| `LoadRecipients.dtsx` | Loads recipient records | 4 (Parallel with Donors) |

## Connection Managers

### OLE DB Connection: OrganDonationDB
```
Provider: SQLOLEDB.1
Server: localhost (or your server name)
Database: OrganDonationDB
Authentication: Windows Authentication
```

### Flat File Connections

**Donors_CSV**
- File: `C:\Data\donors.csv`
- Format: Delimited (comma)
- Text Qualifier: `"`
- Header row in first line: Yes
- All columns: DT_STR (string)

**Recipients_CSV**
- File: `C:\Data\recipients.csv`
- Same configuration as Donors_CSV

**TransplantCenters_CSV**
- File: `C:\Data\transplant_centers.csv`
- Same configuration as Donors_CSV

## Package Variables

All packages should define these variables:

| Variable | Type | Default | Purpose |
|----------|------|---------|---------|
| `LoadID` | Int32 | 0 | Audit tracking |
| `PackageName` | String | (package name) | Audit logging |
| `SourceFileName` | String | (csv filename) | Audit tracking |
| `SourceRowCount` | Int32 | 0 | Row count capture |
| `ErrorRowCount` | Int32 | 0 | Error count capture |

## MasterETL.dtsx Specification

### Control Flow

```
┌─────────────────────────────┐
│   Execute Package Task:     │
│   LoadTransplantCenters     │
└─────────────┬───────────────┘
              │ Success
    ┌─────────┴─────────┐
    │                   │
    ▼                   ▼
┌─────────────┐   ┌─────────────┐
│  Execute    │   │  Execute    │
│  Package:   │   │  Package:   │
│ LoadDonors  │   │LoadRecips   │
└─────────────┘   └─────────────┘
```

### Configuration
- Each Execute Package Task references child .dtsx file
- Precedence constraints: Success only
- Donors and Recipients can execute in parallel (no dependency)

## LoadDonors.dtsx Specification

### Control Flow

```
┌────────────────────────────┐
│ Execute SQL Task:          │
│ "Start Audit Log"          │
│ EXEC audit.usp_StartETLLoad│
│ @PackageName, @SourceFile, │
│ @LoadID OUTPUT             │
└─────────────┬──────────────┘
              │
              ▼
┌────────────────────────────┐
│ Execute SQL Task:          │
│ "Truncate Staging"         │
│ TRUNCATE TABLE stg.Donors; │
│ DELETE FROM error.Donors   │
│ WHERE LoadID = ?           │
└─────────────┬──────────────┘
              │
              ▼
┌────────────────────────────┐
│ Data Flow Task:            │
│ "Load CSV to Staging"      │
│ (See Data Flow below)      │
└─────────────┬──────────────┘
              │
              ▼
┌────────────────────────────┐
│ Execute SQL Task:          │
│ "Validate Data"            │
│ EXEC dbo.usp_ValidateDonors│
│ @LoadID = ?                │
└─────────────┬──────────────┘
              │
              ▼
┌────────────────────────────┐
│ Execute SQL Task:          │
│ "Load to Production"       │
│ EXEC dbo.usp_LoadValidDonors│
│ @LoadID = ?                │
└─────────────┬──────────────┘
              │
              ▼
┌────────────────────────────┐
│ Execute SQL Task:          │
│ "Complete Audit Log"       │
│ EXEC audit.usp_CompleteETL │
│ @LoadID, @Status, counts...│
└────────────────────────────┘
```

### Data Flow: Load CSV to Staging

```
┌──────────────────────────┐
│ Flat File Source         │
│ Connection: Donors_CSV   │
└───────────┬──────────────┘
            │
            ▼
┌──────────────────────────┐
│ Row Count                │
│ Variable: SourceRowCount │
└───────────┬──────────────┘
            │
            ▼
┌──────────────────────────┐
│ Derived Column           │
│ Add columns:             │
│ - LoadID = @LoadID       │
│ - SourceFileName         │
│ - SourceRowNumber        │
└───────────┬──────────────┘
            │
            ▼
┌──────────────────────────┐
│ OLE DB Destination       │
│ Table: stg.Donors        │
│ Fast Load: Yes           │
│ Table Lock: Yes          │
└──────────────────────────┘
```

### Derived Column Expressions

| Column | Expression | Data Type |
|--------|------------|-----------|
| LoadID | `@[User::LoadID]` | DT_I4 |
| SourceFileName | `@[User::SourceFileName]` | DT_STR, 255 |
| SourceRowNumber | Row number (use Script Component for accurate numbering) | DT_I4 |

### Execute SQL Task Parameter Mappings

**Start Audit Log:**
- Parameter 0: `User::PackageName` (Input)
- Parameter 1: `User::SourceFileName` (Input)
- Parameter 2: `User::LoadID` (Output)

**Truncate/Validate/Load:**
- Parameter 0: `User::LoadID` (Input)

**Complete Audit:**
- Multiple parameters mapping LoadID, status, and counts

## LoadRecipients.dtsx Specification

Same structure as LoadDonors.dtsx with these changes:
- Flat File Source: Recipients_CSV
- Staging Table: stg.Recipients
- Validation Procedure: dbo.usp_ValidateRecipients
- Load Procedure: dbo.usp_LoadValidRecipients
- Error Table: error.Recipients

## LoadTransplantCenters.dtsx Specification

Simplified version (no validation - reference data):
1. Start Audit Log
2. Truncate stg.TransplantCenters
3. Data Flow: CSV → Staging
4. Execute SQL: MERGE to dbo.TransplantCenters
5. Complete Audit Log

## Error Handling

### Component Level
- Configure all destinations with "Redirect row" on error
- Create error output path to OLE DB Destination → error tables

### Package Level
- Add OnError event handler
- Log error details to audit table
- Set Status = 'Failed' in ETL_LoadLog

### Precedence Constraints
- Success (Green): Continue on success
- Failure (Red): Route to error handling
- Completion: Always execute (for cleanup tasks)

## Best Practices Implemented

1. **Staging Pattern**: All VARCHAR columns accept any input
2. **Audit Logging**: Every execution tracked with row counts
3. **Error Preservation**: Failed records saved with error details
4. **Idempotent Loads**: MERGE pattern for re-runnable packages
5. **Parameterization**: Variables for all configurable values
6. **Separation of Concerns**: One package per entity

## Testing Checklist

- [ ] All packages execute without errors
- [ ] Audit log captures start/end times and counts
- [ ] Invalid records route to error tables
- [ ] Valid records load to production tables
- [ ] Re-running packages updates (not duplicates) existing records
- [ ] Master package orchestrates correct execution order
