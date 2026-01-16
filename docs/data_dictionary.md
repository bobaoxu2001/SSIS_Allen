# Data Dictionary

This document defines all data elements, valid values, and business rules.

## Source Files

### donors.csv

| Column | Data Type | Required | Description | Valid Values |
|--------|-----------|----------|-------------|--------------|
| UNOS_ID | String | Yes | Unique donor identifier | Format: XXXX-XXXXX |
| FirstName | String | Yes | Donor first name | Non-empty |
| LastName | String | Yes | Donor last name | Non-empty |
| DateOfBirth | Date | Yes | Date of birth | Valid date, age 0-120 |
| BloodType | String | Yes | ABO/Rh blood type | O+, O-, A+, A-, B+, B-, AB+, AB- |
| OrganType | String | Yes | Organ available for donation | Kidney, Liver, Heart, Lung, Pancreas, Intestine |
| ReferralDate | Date | Yes | Date referred to OPO | Valid date, not future |
| OPO_Code | String | Yes | Organ Procurement Organization | Must exist in transplant_centers.csv |
| DonorType | String | Yes | Type of donor | DBD (Brain Death), DCD (Cardiac Death) |
| Status | String | Yes | Current donor status | Active, Inactive, Pending |
| CauseOfDeath | String | No | Cause of death | CVA, Head Trauma, Anoxia, Other |
| Height_cm | Numeric | No | Height in centimeters | 50-250 |
| Weight_kg | Numeric | No | Weight in kilograms | 10-300 |
| ContactPhone | String | No | Contact phone number | Any format |

### recipients.csv

| Column | Data Type | Required | Description | Valid Values |
|--------|-----------|----------|-------------|--------------|
| UNOS_ID | String | Yes | Unique recipient identifier | Format: RCPT-XXXXX |
| FirstName | String | Yes | Recipient first name | Non-empty |
| LastName | String | Yes | Recipient last name | Non-empty |
| DateOfBirth | Date | Yes | Date of birth | Valid date, age 0-120 |
| BloodType | String | Yes | ABO/Rh blood type | O+, O-, A+, A-, B+, B-, AB+, AB- |
| NeededOrgan | String | Yes | Organ needed for transplant | Kidney, Liver, Heart, Lung, Pancreas, Intestine |
| ListingDate | Date | Yes | Date added to waitlist | Valid date, not future |
| OPO_Code | String | Yes | Primary transplant center | Must exist in transplant_centers.csv |
| Status | String | Yes | Waitlist status code | 1, 7 |
| UrgencyCode | String | Yes | UNOS urgency status | 1A, 1B, 2, 3 |
| Diagnosis | String | No | Primary diagnosis | Free text |
| Height_cm | Numeric | No | Height in centimeters | 50-250 |
| Weight_kg | Numeric | No | Weight in kilograms | 10-300 |
| PRA_Percent | Numeric | No | Panel Reactive Antibody % | 0-100 |

### transplant_centers.csv

| Column | Data Type | Required | Description | Valid Values |
|--------|-----------|----------|-------------|--------------|
| OPO_Code | String | Yes | Unique center identifier | 4-character code |
| OPO_Name | String | Yes | Full center name | Non-empty |
| City | String | Yes | City location | Non-empty |
| State | String | Yes | State code | 2-letter US state code |
| Region | Numeric | Yes | UNOS region number | 1-11 |
| CenterType | String | Yes | Type of transplant program | Comprehensive, Kidney-Liver, Kidney-Only |
| CMS_Certification | String | No | CMS certification number | Format varies |
| AccreditationDate | Date | No | Date of accreditation | Valid date |
| Phone | String | No | Contact phone | Any format |
| Email | String | No | Contact email | Valid email format |

## Reference Data

### Blood Types (ABO/Rh System)

| Code | Name | ABO Group | Rh Factor |
|------|------|-----------|-----------|
| O+ | O Positive | O | + |
| O- | O Negative | O | - |
| A+ | A Positive | A | + |
| A- | A Negative | A | - |
| B+ | B Positive | B | + |
| B- | B Negative | B | - |
| AB+ | AB Positive | AB | + |
| AB- | AB Negative | AB | - |

### Organ Types (OPTN Classification)

| Code | Name | Category |
|------|------|----------|
| Kidney | Kidney | Solid Organ |
| Liver | Liver | Solid Organ |
| Heart | Heart | Solid Organ |
| Lung | Lung | Solid Organ |
| Pancreas | Pancreas | Solid Organ |
| Intestine | Intestine | Solid Organ |

### UNOS Urgency Status Codes

| Code | Name | Description |
|------|------|-------------|
| 1A | Status 1A | Highest urgency - immediate need |
| 1B | Status 1B | High urgency |
| 2 | Status 2 | Moderate urgency |
| 3 | Status 3 | Standard priority |

### Donor Types

| Code | Name | Description |
|------|------|-------------|
| DBD | Donation after Brain Death | Donor declared brain dead |
| DCD | Donation after Cardiac Death | Donor declared dead by cardiac criteria |

### UNOS Regions

| Region | States Included |
|--------|-----------------|
| 1 | CT, ME, MA, NH, RI, VT |
| 2 | DE, DC, MD, NJ, PA, WV |
| 3 | AL, AR, FL, GA, LA, MS, PR |
| 4 | OK, TX |
| 5 | AZ, CA, NV, NM, UT |
| 6 | AK, HI, ID, MT, OR, WA |
| 7 | IL, MN, ND, SD, WI |
| 8 | CO, IA, KS, MO, NE, WY |
| 9 | NY, VT |
| 10 | IN, MI, OH |
| 11 | KY, NC, SC, TN, VA |

## Validation Rules

### VLD001: Required Fields
- **Fields**: UNOS_ID, FirstName, LastName
- **Rule**: Must not be NULL or empty string
- **Error Code**: VLD001

### VLD002: Blood Type Validity
- **Field**: BloodType
- **Rule**: Must match one of: O+, O-, A+, A-, B+, B-, AB+, AB-
- **Error Code**: VLD002

### VLD003: Date Validity
- **Fields**: DateOfBirth, ReferralDate/ListingDate
- **Rules**:
  - Must be valid date format
  - DateOfBirth must result in age 0-120
  - ReferralDate/ListingDate must not be in future
- **Error Code**: VLD003

### VLD004: OPO Code Referential Integrity
- **Field**: OPO_Code
- **Rule**: Must exist in TransplantCenters table
- **Error Code**: VLD004

### VLD005: Status Validity
- **Field**: Status (Donors)
- **Rule**: Must be one of: Active, Inactive, Pending
- **Error Code**: VLD005

### VLD006: Donor Type Validity
- **Field**: DonorType
- **Rule**: Must be one of: DBD, DCD
- **Error Code**: VLD006

### VLD007: Urgency Code Validity
- **Field**: UrgencyCode (Recipients)
- **Rule**: Must be one of: 1A, 1B, 2, 3
- **Error Code**: VLD007

## Derived/Calculated Fields

| Field | Calculation | Table |
|-------|-------------|-------|
| Age | DATEDIFF(YEAR, DateOfBirth, GETDATE()) | Donors, Recipients |
| DaysOnWaitlist | DATEDIFF(DAY, ListingDate, GETDATE()) | Recipients |
| BMI | Weight_kg / (Height_cm/100)² | Donors, Recipients |

## Data Sources

- **OPTN**: Organ Procurement and Transplantation Network - https://optn.transplant.hrsa.gov/
- **UNOS**: United Network for Organ Sharing - https://unos.org/
- **SRTR**: Scientific Registry of Transplant Recipients - https://www.srtr.org/
