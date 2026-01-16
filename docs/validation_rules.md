# Validation Rules Documentation

This document details all data validation rules implemented in the ETL pipeline.

## Validation Philosophy

### Principles
1. **Never lose data** - Invalid records are captured, not discarded
2. **Fail early, fail loud** - Catch errors at staging, before production load
3. **Specific error messages** - Each error includes the exact field and reason
4. **Audit trail** - All errors linked to LoadID for traceability

### Validation Order
Validations execute in sequence. Once a record fails, it is excluded from subsequent validations to avoid duplicate error entries.

## Error Code Reference

| Code | Category | Description |
|------|----------|-------------|
| VLD001 | Completeness | Required field missing |
| VLD002 | Validity | Blood type invalid |
| VLD003 | Validity | Date invalid or out of range |
| VLD004 | Referential | Foreign key not found |
| VLD005 | Domain | Status code invalid |
| VLD006 | Domain | Donor type invalid |
| VLD007 | Domain | Urgency code invalid |

## Detailed Validation Rules

### VLD001: Required Field Validation

**Purpose**: Ensure all mandatory fields have values

**Fields Checked**:
- UNOS_ID
- FirstName
- LastName

**Logic**:
```sql
WHERE UNOS_ID IS NULL 
   OR LTRIM(RTRIM(UNOS_ID)) = ''
   OR FirstName IS NULL 
   OR LTRIM(RTRIM(FirstName)) = ''
   OR LastName IS NULL 
   OR LTRIM(RTRIM(LastName)) = ''
```

**Business Justification**: All records must be identifiable. Anonymous records cannot be processed or matched.

---

### VLD002: Blood Type Validation

**Purpose**: Ensure blood type is medically valid

**Valid Values**: O+, O-, A+, A-, B+, B-, AB+, AB-

**Logic**:
```sql
WHERE NOT EXISTS (
    SELECT 1 FROM dbo.BloodTypes bt 
    WHERE bt.BloodTypeCode = source.BloodType
)
```

**Business Justification**: Invalid blood types would cause matching failures or dangerous transplant attempts.

---

### VLD003: Date Validation

**Purpose**: Ensure dates are valid and logically consistent

**Rules**:
1. Date must be parseable (not 'abc' or '2024-13-45')
2. Date of birth must not be in future
3. Age derived from DOB must be 0-120 years
4. Referral/Listing date must not be in future

**Logic**:
```sql
WHERE TRY_CAST(DateOfBirth AS DATE) IS NULL
   OR TRY_CAST(DateOfBirth AS DATE) > GETDATE()
   OR DATEDIFF(YEAR, TRY_CAST(DateOfBirth AS DATE), GETDATE()) > 120
```

**Business Justification**: Invalid dates indicate data entry errors and would corrupt derived calculations (age, days on waitlist).

---

### VLD004: OPO Code Referential Integrity

**Purpose**: Ensure all records reference valid transplant centers

**Logic**:
```sql
WHERE NOT EXISTS (
    SELECT 1 FROM dbo.TransplantCenters tc 
    WHERE tc.OPO_Code = source.OPO_Code 
      AND tc.IsActive = 1
)
```

**Business Justification**: Records with invalid OPO codes cannot be assigned to a center, breaking reporting and operational workflows.

---

### VLD005: Status Validation (Donors)

**Purpose**: Ensure donor status is standardized

**Valid Values**: Active, Inactive, Pending

**Logic**:
```sql
WHERE Status NOT IN ('Active', 'Inactive', 'Pending')
```

**Business Justification**: Non-standard status values would cause inconsistent filtering in reports and matching logic.

---

### VLD006: Donor Type Validation

**Purpose**: Ensure donor type follows OPTN classification

**Valid Values**: DBD (Brain Death), DCD (Cardiac Death)

**Logic**:
```sql
WHERE DonorType NOT IN ('DBD', 'DCD')
```

**Business Justification**: Donor type affects organ viability and matching protocols.

---

### VLD007: Urgency Code Validation (Recipients)

**Purpose**: Ensure urgency follows UNOS status codes

**Valid Values**: 1A, 1B, 2, 3

**Logic**:
```sql
WHERE UrgencyCode NOT IN ('1A', '1B', '2', '3')
```

**Business Justification**: Urgency determines allocation priority. Invalid codes would corrupt waitlist ordering.

## Error Table Structure

```sql
CREATE TABLE error.Donors (
    ErrorID INT IDENTITY PRIMARY KEY,
    LoadID INT NOT NULL,
    
    -- Original source data (preserved exactly)
    UNOS_ID VARCHAR(50),
    FirstName VARCHAR(100),
    -- ... all source columns ...
    
    -- Error details
    ErrorCode VARCHAR(20) NOT NULL,
    ErrorColumn VARCHAR(100),
    ErrorDescription VARCHAR(500) NOT NULL,
    ErrorDateTime DATETIME DEFAULT GETDATE(),
    SourceRowNumber INT
);
```

## Sample Error Output

| UNOS_ID | ErrorCode | ErrorColumn | ErrorDescription |
|---------|-----------|-------------|------------------|
| XXXX-00001 | VLD003 | DateOfBirth | Invalid or missing date of birth: 'NULL' |
| YYYY-00002 | VLD002 | BloodType | Invalid blood type: 'X+'. Valid values: O+, O-, A+, A-, B+, B-, AB+, AB- |
| ZZZZ-00003 | VLD003 | DateOfBirth | Date of birth cannot be in the future: 2030-07-22 |
| AAAA-00004 | VLD004 | OPO_Code | OPO code not found in TransplantCenters: 'INVALID' |

## Validation Reporting

### Error Summary Query
```sql
SELECT 
    ErrorCode,
    ErrorColumn,
    COUNT(*) AS ErrorCount
FROM error.Donors
WHERE LoadID = @LoadID
GROUP BY ErrorCode, ErrorColumn
ORDER BY ErrorCount DESC;
```

### Error Rate Calculation
```sql
SELECT 
    LoadID,
    SourceRowCount,
    ErrorRowCount,
    CAST(ErrorRowCount * 100.0 / NULLIF(SourceRowCount, 0) AS DECIMAL(5,2)) AS ErrorRate
FROM audit.ETL_LoadLog
WHERE LoadID = @LoadID;
```

## Extending Validation Rules

To add a new validation:

1. Define error code (VLDxxx)
2. Add INSERT statement to validation procedure
3. Document in this file
4. Test with sample bad data
5. Verify error captures correctly

Example:
```sql
-- VLD008: Email Format Validation
INSERT INTO error.Donors (...)
SELECT ...
WHERE Email IS NOT NULL 
  AND Email NOT LIKE '%_@__%.__%'
```
