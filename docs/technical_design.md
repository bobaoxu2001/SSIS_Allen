# Technical Design Document

## 1. Overview

This document describes the technical architecture and design decisions for the Organ Donation ETL Pipeline.

## 2. Architecture

### 2.1 High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                            SOURCE LAYER                                  │
│                                                                          │
│   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐               │
│   │  donors.csv  │   │recipients.csv│   │ centers.csv  │               │
│   │  (35 rows)   │   │  (29 rows)   │   │  (10 rows)   │               │
│   └──────┬───────┘   └──────┬───────┘   └──────┬───────┘               │
│          │                  │                  │                        │
└──────────┼──────────────────┼──────────────────┼────────────────────────┘
           │                  │                  │
           │    SSIS Flat File Connections      │
           ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         ETL LAYER (SSIS)                                 │
│                                                                          │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │                    MasterETL.dtsx                                │   │
│   │                                                                  │   │
│   │    ┌──────────────┐                                             │   │
│   │    │    Start     │                                             │   │
│   │    └──────┬───────┘                                             │   │
│   │           │                                                     │   │
│   │           ▼                                                     │   │
│   │    ┌──────────────┐                                             │   │
│   │    │LoadCenters   │  (Reference data first)                     │   │
│   │    └──────┬───────┘                                             │   │
│   │           │                                                     │   │
│   │    ┌──────┴──────┐                                              │   │
│   │    │             │                                              │   │
│   │    ▼             ▼                                              │   │
│   │ ┌──────────┐ ┌──────────┐                                       │   │
│   │ │LoadDonors│ │LoadRecips│  (Parallel execution)                 │   │
│   │ └──────────┘ └──────────┘                                       │   │
│   │                                                                  │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
           │
           │    OLE DB Connection
           ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       DATABASE LAYER                                     │
│                                                                          │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │                    STAGING (stg schema)                          │   │
│   │                                                                  │   │
│   │   • All VARCHAR columns                                         │   │
│   │   • Accepts any source data                                     │   │
│   │   • ETL metadata: LoadID, SourceFile, RowNumber                 │   │
│   │                                                                  │   │
│   └──────────────────────────┬──────────────────────────────────────┘   │
│                              │                                          │
│                              │  Stored Procedures                       │
│                              ▼                                          │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │                   VALIDATION                                     │   │
│   │                                                                  │   │
│   │   • usp_ValidateDonors                                          │   │
│   │   • usp_ValidateRecipients                                      │   │
│   │   • Completeness, validity, referential checks                  │   │
│   │                                                                  │   │
│   └──────────────┬────────────────────────────┬─────────────────────┘   │
│                  │                            │                         │
│          Valid Records                 Invalid Records                  │
│                  │                            │                         │
│                  ▼                            ▼                         │
│   ┌──────────────────────┐     ┌──────────────────────┐                │
│   │  PRODUCTION (dbo)    │     │   ERROR (error)      │                │
│   │                      │     │                      │                │
│   │  • Proper data types │     │  • Original data     │                │
│   │  • FK constraints    │     │  • Error code/desc   │                │
│   │  • Check constraints │     │  • Source row number │                │
│   │  • Indexes           │     │                      │                │
│   └──────────────────────┘     └──────────────────────┘                │
│                                                                         │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │                    AUDIT (audit schema)                          │   │
│   │                                                                  │   │
│   │   • ETL_LoadLog: Every execution logged                         │   │
│   │   • Row counts: source, staged, inserted, errors                │   │
│   │   • Duration, status, executor                                  │   │
│   │                                                                  │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Schema Organization

| Schema | Purpose | Tables |
|--------|---------|--------|
| `dbo` | Production data and reference tables | BloodTypes, OrganTypes, StatusCodes, TransplantCenters, Donors, Recipients |
| `stg` | Staging area for raw data | Donors, Recipients, TransplantCenters |
| `error` | Failed record capture | Donors, Recipients |
| `audit` | ETL execution logging | ETL_LoadLog |

## 3. Design Patterns

### 3.1 Staging Pattern

**Problem**: Source data may contain invalid formats that cause load failures.

**Solution**: Stage all data as VARCHAR first, then validate and transform.

**Benefits**:
- No data loss from type conversion failures
- Original source data preserved for audit
- Validation happens in controlled environment
- Errors are captured with context

### 3.2 Error Redirection Pattern

**Problem**: Traditional ETL fails entire batch on single bad record.

**Solution**: Redirect invalid records to error tables, continue processing valid records.

**Benefits**:
- Partial success rather than total failure
- Error analysis without data loss
- Production data remains clean
- Source system feedback enabled

### 3.3 MERGE (Upsert) Pattern

**Problem**: Multiple loads may process same records; need to avoid duplicates.

**Solution**: Use MERGE statement to insert new records and update existing.

**Benefits**:
- Idempotent operations (safe to re-run)
- Single statement for insert/update
- Maintains referential integrity
- Supports incremental loading

### 3.4 Audit Logging Pattern

**Problem**: Compliance requires tracking what data was loaded when.

**Solution**: Log every ETL execution with metadata and row counts.

**Benefits**:
- Full traceability for compliance
- Reconciliation support (source = staged + errors)
- Performance monitoring
- Troubleshooting support

## 4. Database Design

### 4.1 Table Relationships

```
┌──────────────┐
│  BloodTypes  │◄─────────────────────────────────┐
│  (Reference) │                                  │
└──────────────┘                                  │
                                                  │
┌──────────────┐                                  │
│  OrganTypes  │◄────────────────────────────┐    │
│  (Reference) │                             │    │
└──────────────┘                             │    │
                                             │    │
┌──────────────┐                             │    │
│ StatusCodes  │◄───────────────────────┐    │    │
│  (Reference) │                        │    │    │
└──────────────┘                        │    │    │
                                        │    │    │
┌──────────────────┐                    │    │    │
│ TransplantCenters│◄──────────────┐    │    │    │
│   (Dimension)    │               │    │    │    │
└──────────────────┘               │    │    │    │
                                   │    │    │    │
                              ┌────┴────┴────┴────┴────┐
                              │                        │
                         ┌────┴─────┐            ┌─────┴────┐
                         │  Donors  │            │Recipients│
                         │  (Fact)  │            │  (Fact)  │
                         └──────────┘            └──────────┘
```

### 4.2 Indexing Strategy

| Table | Index | Columns | Purpose |
|-------|-------|---------|---------|
| Donors | IX_Donors_BloodType | BloodType | Matching queries |
| Donors | IX_Donors_OrganType | OrganType | Matching queries |
| Donors | IX_Donors_Status | Status (filtered) | Active donor queries |
| Recipients | IX_Recipients_UrgencyCode | UrgencyCode | Priority sorting |
| Recipients | IX_Recipients_ListingDate | ListingDate | Waitlist ordering |

## 5. ETL Flow Detail

### 5.1 Package Execution Sequence

```
1. MasterETL.dtsx
   │
   ├─► 2. LoadTransplantCenters.dtsx
   │       ├─► Start Audit Log
   │       ├─► Truncate Staging
   │       ├─► Load CSV → Staging
   │       ├─► MERGE → Production
   │       └─► Complete Audit Log
   │
   └─► (After Centers completes)
       │
       ├─► 3. LoadDonors.dtsx (parallel)
       │       ├─► Start Audit Log
       │       ├─► Truncate Staging
       │       ├─► Load CSV → Staging
       │       ├─► Validate (stored proc)
       │       ├─► Load Valid → Production
       │       └─► Complete Audit Log
       │
       └─► 4. LoadRecipients.dtsx (parallel)
               ├─► Start Audit Log
               ├─► Truncate Staging
               ├─► Load CSV → Staging
               ├─► Validate (stored proc)
               ├─► Load Valid → Production
               └─► Complete Audit Log
```

### 5.2 Data Flow Components

| Component | Purpose | Configuration |
|-----------|---------|---------------|
| Flat File Source | Read CSV | All columns as DT_STR |
| Row Count | Capture count | Store in variable |
| Derived Column | Add metadata | LoadID, SourceFileName |
| OLE DB Destination | Write to staging | Fast Load enabled |

## 6. Error Handling

### 6.1 Component Level
- All destinations configured with "Redirect row" on error
- Error output routes to error tables

### 6.2 Package Level
- OnError event handler captures failures
- Updates audit log with error message
- Sets Status = 'Failed'

### 6.3 Validation Level
- Stored procedures insert to error tables
- Each error has code, column, description
- Original data preserved exactly

## 7. Performance Considerations

| Technique | Implementation | Impact |
|-----------|---------------|--------|
| Fast Load | OLE DB Destination | Bypasses row-by-row |
| Table Lock | Destination option | Reduces lock overhead |
| Batch Commit | Default batch size | Balanced memory/commits |
| Filtered Index | WHERE Status = 'Active' | Faster active queries |
| Staging Truncate | TRUNCATE vs DELETE | Minimal logging |

## 8. Security Considerations

- Windows Authentication for database connections
- No credentials stored in packages
- Audit log captures executor identity
- Error tables may contain PII - access controlled

## 9. Deployment

### 9.1 Development
- Visual Studio project deployment model
- Local SQL Server for testing

### 9.2 Production
- Deploy to SSIS Catalog (SSISDB)
- Environment variables for connection strings
- SQL Agent jobs for scheduling
- Alerting on failures

## 10. Future Enhancements

1. **Incremental Loading**: Add change detection (CDC or timestamps)
2. **Parameterization**: Environment-specific connection strings
3. **Automated Testing**: Unit tests for validation procedures
4. **Data Profiling**: Baseline quality metrics
5. **Alerting**: Email notifications on failures or high error rates
