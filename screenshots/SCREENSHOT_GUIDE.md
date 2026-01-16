# Screenshot Capture Guide

Follow this guide to capture screenshots that demonstrate your SSIS project.

## Required Screenshots (10-15 minutes to capture)

### 1. SSMS - Database Structure
**File name**: `01_database_structure.png`

**Steps**:
1. Open SSMS, connect to localhost
2. Expand OrganDonationDB → Tables
3. Take screenshot showing all tables organized by schema (dbo, stg, error, audit)

**What it shows**: Proper database organization with multiple schemas

---

### 2. SSMS - Table Design
**File name**: `02_table_design.png`

**Steps**:
1. Right-click dbo.Donors → Design
2. Take screenshot showing columns, data types, and constraints

**What it shows**: Proper data modeling with constraints

---

### 3. SSMS - Query Results (Audit Log)
**File name**: `03_audit_log_results.png`

**Steps**:
1. Run: `SELECT * FROM audit.ETL_LoadLog ORDER BY LoadID DESC;`
2. Screenshot the results grid

**What it shows**: ETL execution tracking with row counts

---

### 4. SSMS - Query Results (Error Table)
**File name**: `04_error_capture.png`

**Steps**:
1. Run: `SELECT UNOS_ID, ErrorCode, ErrorColumn, ErrorDescription FROM error.Donors;`
2. Screenshot showing captured errors

**What it shows**: Error handling - bad data captured with reasons

---

### 5. SSMS - Query Results (Production Data)
**File name**: `05_production_data.png`

**Steps**:
1. Run: `SELECT TOP 10 * FROM dbo.Donors WHERE Status = 'Active';`
2. Screenshot the clean production data

**What it shows**: Valid data successfully loaded

---

### 6. Visual Studio - SSIS Control Flow
**File name**: `06_ssis_control_flow.png`

**Steps**:
1. Open LoadDonors.dtsx in Visual Studio
2. Show Control Flow tab with all tasks connected

**What it shows**: ETL orchestration design

---

### 7. Visual Studio - SSIS Data Flow
**File name**: `07_ssis_data_flow.png`

**Steps**:
1. Double-click Data Flow Task
2. Show: Flat File Source → Row Count → Derived Column → OLE DB Destination

**What it shows**: Data transformation pipeline

---

### 8. Visual Studio - Package Execution (Success)
**File name**: `08_package_execution.png`

**Steps**:
1. Run the MasterETL.dtsx package
2. Screenshot when all tasks show green checkmarks

**What it shows**: Successful ETL execution

---

### 9. SSMS - Executive Summary View
**File name**: `09_executive_summary.png`

**Steps**:
1. Run: `SELECT * FROM dbo.vw_ExecutiveSummary;`
2. Screenshot the KPI results

**What it shows**: Reporting capability

---

### 10. SSMS - Potential Matches Query
**File name**: `10_donor_recipient_matches.png`

**Steps**:
1. Run the potential matches query from 06_reporting_views.sql
2. Screenshot showing donor-recipient matching

**What it shows**: Business value - actionable insights

---

## Quick Capture Checklist

- [ ] 01_database_structure.png
- [ ] 02_table_design.png
- [ ] 03_audit_log_results.png
- [ ] 04_error_capture.png
- [ ] 05_production_data.png
- [ ] 06_ssis_control_flow.png
- [ ] 07_ssis_data_flow.png
- [ ] 08_package_execution.png
- [ ] 09_executive_summary.png
- [ ] 10_donor_recipient_matches.png

## Tips

1. Use **Snipping Tool** (Win+Shift+S) for clean screenshots
2. Make sure results are **clearly visible** (adjust column widths)
3. Include **timestamps** in audit results to show recent execution
4. Use **consistent window sizing** for professional look
