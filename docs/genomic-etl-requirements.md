# Genomic Expression Data ETL - Requirements Document

**Document Version**: 1.0  
**Date**: October 14, 2025  
**Requirements Analyst**: Technical Specifications  
**Status**: Final Requirements  

## Overview

This document defines the functional and technical requirements for a genomic expression data Extract, Transform, and Load (ETL) system. The ETL will process multiple genomic studies from TSV files containing expression data and metadata, filter the data based on predefined Ensembl gene identifiers, and populate a normalized database schema for analytical querying.

The system is designed to handle the complexities of genomic research data, including inconsistent metadata schemas, large datasets, and the need for reliable batch processing with error recovery capabilities.

## Goals

### Primary Goals
1. **Automated Data Processing**: Create a robust ETL pipeline that processes multiple genomic studies concurrently without manual intervention
2. **Data Quality Management**: Transform messy, inconsistent genomic metadata into clean, standardized dimensional tables  
3. **Performance Optimization**: Filter large expression datasets during ingestion to process only relevant genes, improving memory efficiency and processing speed
4. **System Reliability**: Provide resilient processing with connection failure recovery and idempotent operations
5. **Analytical Database Population**: Populate a star schema database optimized for genomic expression analysis

### Secondary Goals
- Configurable processing parameters for different deployment environments
- Comprehensive monitoring and logging for operational visibility
- Scalable architecture supporting varying study sizes and concurrent processing

## Assumptions

### Data Assumptions
- **File Format**: All input files are in TSV (tab-separated values) format
- **Study Organization**: Each study resides in its own directory with standardized file naming conventions
- **Metadata Variability**: Different studies may have varying metadata schemas, but all contain core required fields
- **Gene Identification**: All expression data uses Ensembl gene identifiers (format: ENSG00000XXXXXXX)
- **Data Volume**: Individual studies may contain 10,000-50,000 samples with expression data for 20,000+ genes

### Infrastructure Assumptions  
- SQL Server database with sufficient capacity for dimensional tables and fact data
- File system access to organized study directories
- Network connectivity allowing resume capability after temporary disconnections
- Sufficient memory for batch processing (minimum 8GB recommended)

### Processing Assumptions
- Studies can be processed independently and concurrently
- Gene filtering reduces dataset size by approximately 99% (from ~20,000 genes to ~120 target genes)
- Database supports batch inserts and transaction management
- Configuration files are maintained and accessible to the ETL process

## Requirements

### MUST HAVE Requirements

#### Core ETL Functionality

**REQ-001: Multi-Study Processing**
- **Description**: The system must process multiple genomic studies concurrently from an organized folder structure
- **Details**: 
  - Each study folder contains: expression data TSV file, metadata TSV file
  - System scans root directory for study folders automatically
  - Concurrent processing of multiple studies with configurable thread limits
- **Acceptance Criteria**: Successfully processes at least 5 studies concurrently without data corruption

**REQ-002: Gene Filtering During Load**  
- **Description**: The system must filter expression data by Ensembl gene IDs during data ingestion to optimize performance
- **Details**:
  - Load gene filter list from external configuration file
  - Filter format: `ensembl_id` column containing ENSG identifiers
  - Skip non-matching genes without individual logging
  - Apply filtering before loading into memory structures
- **Input Example**: Filter file contains genes like `ENSG00000115977`, `ENSG00000112304`, `ENSG00000093072`
- **Acceptance Criteria**: Only genes present in filter list are processed and loaded to database

**REQ-003: Database Schema Population**
- **Description**: The system must populate all five dimensional tables with proper foreign key relationships
- **Target Schema**:
  ```sql
  -- Fact table
  fact_expression: sample_key, gene_key, expression_value, study_key
  
  -- Dimension tables  
  dim_gene: gene_key (identity), ensembl_id
  dim_sample: sample_key (identity), gsm_accession, study_key, platform_key, illness_key, age, sex
  dim_study: study_key (identity), gse_accession  
  dim_illness: illness_key (identity), illness_label
  dim_platform: platform_key (identity), platform_accession
  ```
- **Acceptance Criteria**: All tables populated with correct data types and valid foreign key relationships

**REQ-004: Data Transformation Logic**
- **Description**: The system must apply specific transformation rules for metadata extraction and standardization
- **Source-to-Target Mapping**:
  ```
  # Sample Metadata Mapping
  gsm_accession ← refinebio_accession_code
  age ← refinebio_age OR characteristics_ch1_Age  
  sex ← refinebio_sex OR characteristics_ch1_Sex OR characteristics_ch1_Gender
  illness_label ← characteristics_ch1_Illness
  platform_accession ← refinebio_platform
  gse_accession ← experiment_accession (study level)
  
  # Expression Data Mapping  
  ensembl_id ← Gene column (first column in expression file)
  expression_value ← Sample columns (GSM228562, GSM228563, etc.)
  ```
- **Missing Value Handling**: Replace empty/null values with "UNKNOWN" string
- **Acceptance Criteria**: Transformed data matches expected mappings with proper "UNKNOWN" substitution

**REQ-005: Duplicate Handling**
- **Description**: The system must skip duplicate records on re-processing while maintaining data integrity
- **Duplicate Detection Logic**:
  - **Studies**: Skip if `gse_accession` already exists in dim_study
  - **Samples**: Skip if combination of `gsm_accession` + `study_key` exists in dim_sample  
  - **Genes**: Skip if `ensembl_id` already exists in dim_gene
  - **Expression Facts**: Skip if combination of `sample_key` + `gene_key` + `study_key` exists
- **Acceptance Criteria**: Re-running ETL on same data produces no duplicate records

**REQ-006: Batch Processing & Transaction Management**
- **Description**: The system must process data in configurable batches with proper transaction handling
- **Batch Configuration**:
  - Default batch size: 1000 records  
  - Configurable via configuration file
  - Separate batch processing for each table type
- **Transaction Scope**: Each batch commits as separate transaction for memory management
- **Acceptance Criteria**: System processes 10,000+ records in 1000-record batches without memory issues

**REQ-007: Resume Capability**  
- **Description**: The system must resume processing after database connection failures without restarting entire studies
- **Resume Logic**:
  - Track processing state per study in database or state file
  - Detect incomplete studies on startup  
  - Resume from last successful batch commit
  - Handle both connection timeouts and network failures
- **Acceptance Criteria**: System resumes processing after simulated connection failure without data loss or duplication

**REQ-008: Configuration Management**
- **Description**: The system must use external configuration files for all deployment-specific parameters
- **Configuration Parameters**:
  ```yaml
  # Example config.yaml
  database:
    connection_string: "Server=localhost;Database=db_genes;Trusted_Connection=true;"
    batch_size: 1000
    connection_timeout: 30
    
  processing:
    input_directory: "/data/studies"
    gene_filter_file: "/config/filter_genes.tsv"
    max_concurrent_studies: 3
    
  logging:
    log_level: "INFO"
    log_directory: "/logs"
    log_processing_time: true
    log_record_counts: true
    log_data_quality: true
  ```
- **Acceptance Criteria**: All parameters configurable without code changes

#### Monitoring & Logging

**REQ-009: Process Monitoring**  
- **Description**: The system must provide comprehensive logging for operations monitoring
- **Required Logging**:
  - Processing start/end time per study
  - Record counts (loaded, filtered, errors) per table per study  
  - Data quality issues (missing required fields, format problems)
  - Performance metrics (records/second, memory usage)
  - **Excluded**: Individual skipped gene records (performance optimization)
- **Acceptance Criteria**: Log files contain sufficient information for troubleshooting and performance monitoring

### SHOULD HAVE Requirements

#### Enhanced Error Handling

**REQ-010: Validation Reporting**
- **Description**: The system should generate summary reports of data quality issues per study
- **Report Contents**:
  - Percentage of samples with complete metadata
  - Count of genes filtered vs. total genes in source
  - List of studies with processing warnings
  - Performance statistics (processing time, throughput)
- **Acceptance Criteria**: Summary report generated after each ETL run

**REQ-011: File Format Validation**  
- **Description**: The system should validate TSV file structure before processing
- **Validation Checks**:
  - Verify required columns exist in metadata files
  - Check expression file has gene column plus sample columns
  - Validate file encoding and delimiter consistency  
  - Confirm file size within expected ranges
- **Acceptance Criteria**: Processing stops with clear error message for invalid file formats

**REQ-012: Performance Optimization**
- **Description**: The system should support parallel processing optimizations for large datasets
- **Optimization Features**:
  - Parallel study processing with configurable thread pools
  - Bulk insert operations for database loading
  - Memory-efficient streaming for large TSV files
  - Connection pooling for database operations
- **Acceptance Criteria**: 50% improvement in processing time for multiple concurrent studies

#### Configuration Enhancements

**REQ-013: Dynamic Column Mapping**
- **Description**: The system should support configurable metadata field mappings for different study types  
- **Configuration Example**:
  ```yaml
  field_mappings:
    age_fields: ["refinebio_age", "characteristics_ch1_Age", "characteristics_ch1_age", "MetaSRA_age"]
    sex_fields: ["refinebio_sex", "characteristics_ch1_Sex", "characteristics_ch1_Gender"]
    illness_fields: ["characteristics_ch1_Illness", "refinebio_disease"]
    platform_fields: ["refinebio_platform", "platform_id"]
  ```
- **Acceptance Criteria**: System successfully processes studies with varying metadata column names

### WON'T HAVE Requirements

#### Explicitly Out of Scope

**REQ-014: Real-time Processing**  
- **Rationale**: Genomic research workflows operate on batch schedules; real-time streaming adds unnecessary complexity
- **Alternative**: Scheduled batch processing meets research requirements

**REQ-015: Advanced Data Validation**
- **Rationale**: Research requirement to import expression data "as-is" without range validation  
- **Alternative**: Downstream analysis tools handle data quality validation

**REQ-016: Web Interface**
- **Rationale**: Technical users prefer command-line automation and scripting capabilities
- **Alternative**: Command-line interface with comprehensive logging

**REQ-017: Data Versioning**  
- **Rationale**: Source TSV files serve as system of record; database versioning adds storage overhead
- **Alternative**: Maintain source file archives for historical analysis

**REQ-018: Custom Export Formats**
- **Rationale**: Database serves as single analytical data store; export features not required
- **Alternative**: Direct database query access for downstream tools

## User Stories

### Data Analyst User Stories

**US-001: Process Multiple Studies**
> **As a** genomics data analyst  
> **I want to** process multiple expression studies simultaneously  
> **So that** I can efficiently load large datasets without manual intervention  
>
> **Acceptance Criteria:**
> - I can configure the system to process 5 studies concurrently
> - Each study processes independently without interfering with others  
> - I receive a summary report showing processing results for all studies
> - Processing completes in under 2 hours for typical study sizes

**US-002: Filter Relevant Genes**  
> **As a** genomics researcher  
> **I want to** load only genes relevant to my analysis  
> **So that** I can reduce storage requirements and improve query performance
>
> **Acceptance Criteria:**
> - I can specify a gene filter file containing my target Ensembl IDs
> - The system loads only expression data for genes in my filter list
> - I can see in the logs how many genes were filtered vs. total genes available
> - Database queries run faster due to reduced data volume

**US-003: Handle Connection Failures**
> **As a** data operations engineer  
> **I want to** resume processing after database connection failures  
> **So that** long-running ETL jobs don't fail completely due to temporary network issues
>
> **Acceptance Criteria:**  
> - If database connection is lost during processing, the system detects the failure
> - The system waits and retries connection with exponential backoff
> - Processing resumes from the last successful batch commit
> - No duplicate data is created when resuming

### System Administrator User Stories

**US-004: Configure Processing Parameters**
> **As a** system administrator  
> **I want to** configure ETL parameters without modifying code  
> **So that** I can adapt the system to different environments and requirements
>
> **Acceptance Criteria:**
> - All database connections, file paths, and processing options are in config files
> - I can change batch sizes, concurrent processing limits, and timeout values  
> - Configuration changes take effect without recompilation
> - Invalid configuration values produce clear error messages

**US-005: Monitor Processing Performance**  
> **As a** system administrator  
> **I want to** monitor ETL performance and data quality  
> **So that** I can identify bottlenecks and ensure reliable data processing
>
> **Acceptance Criteria:**
> - Log files show processing time, record counts, and error rates for each study
> - I can track memory usage and database performance during large runs
> - Data quality issues are logged with sufficient detail for investigation  
> - Performance trends are visible across multiple ETL runs

### Database Developer User Stories

**US-006: Populate Dimensional Schema**
> **As a** database developer  
> **I want to** ensure proper foreign key relationships in the loaded data  
> **So that** analytical queries join correctly across dimensional tables
>
> **Acceptance Criteria:**
> - All foreign keys in fact_expression reference valid dimension records
> - Dimension tables contain no orphaned records  
> - Surrogate keys are properly generated and unique across studies
> - Database constraints validate referential integrity

## Example Processing Flows

### Flow 1: Successful Multi-Study Processing

```
1. System Startup
   ├── Load configuration from config.yaml
   ├── Validate database connectivity  
   ├── Load gene filter list (120 target genes)
   └── Scan /data/studies directory

2. Discovery Phase  
   ├── Found studies: GSE9006/, GSE10201/, GSE15061/
   ├── Validate each study has required TSV files
   └── Queue studies for processing

3. Concurrent Processing (max 3 studies)
   ├── Study GSE9006: Start processing
   │   ├── Load metadata_GSE9006.tsv (163 samples)
   │   ├── Extract sample dimensions → dim_sample, dim_illness, dim_platform
   │   ├── Load expression_GSE9006.tsv (22,283 genes) 
   │   ├── Filter to target genes (120 genes matched)
   │   └── Load expression facts (163 samples × 120 genes = 19,560 records)
   │
   ├── Study GSE10201: Start processing (parallel)
   └── Study GSE15061: Start processing (parallel)

4. Batch Commits (per study)
   ├── Commit dim_sample records in batches of 1000
   ├── Commit fact_expression records in batches of 1000  
   └── Update processing state after each successful batch

5. Completion
   ├── Generate summary report
   ├── Log final statistics
   └── Exit with success code
```

### Flow 2: Recovery from Connection Failure

```
1. Processing in Progress
   ├── Study GSE9006: 50% complete (9,780 expression records loaded)
   ├── Study GSE10201: 25% complete  
   └── Database connection lost

2. Error Detection & Recovery
   ├── Detect connection failure
   ├── Log error with current processing state
   ├── Wait 30 seconds, attempt reconnection
   └── Successful reconnection established

3. Resume Processing  
   ├── Query database for last successful batch per study
   ├── Study GSE9006: Resume from record 9,801  
   ├── Study GSE10201: Resume from record 2,501
   └── Continue processing remaining records

4. Validation
   ├── Verify no duplicate records created
   ├── Confirm all expected records loaded
   └── Complete processing normally
```

### Flow 3: Data Quality Handling

```
1. Metadata Processing
   ├── Load metadata_GSE9006.tsv
   ├── Sample GSM228562: characteristics_ch1_Age = "16"
   ├── Sample GSM228563: characteristics_ch1_Age = "" (empty)
   │   └── Set age = "UNKNOWN" 
   ├── Sample GSM228564: characteristics_ch1_Illness = "Healthy"
   └── Sample GSM228565: characteristics_ch1_Sex missing
       └── Set sex = "UNKNOWN"

2. Gene Filtering
   ├── Expression file contains 22,283 genes
   ├── Gene ENSG00000115977 (AAK1): Match in filter → Include
   ├── Gene ENSG00000000003: Not in filter → Skip (no log)
   ├── Gene ENSG00000112304 (ACOT13): Match in filter → Include
   └── Final result: 120 genes processed, 22,163 genes skipped

3. Quality Reporting
   ├── Log: "Processed 163 samples, 120 genes, 19,560 expression records"
   ├── Log: "Data quality: 89% samples with complete age, 92% with complete sex"
   └── Log: "Processing time: 4.2 minutes, 78 records/second"
```

## Directory Structure Examples

### Input Directory Structure
```
/data/studies/
├── GSE9006/
│   ├── metadata_GSE9006.tsv          # Sample metadata  
│   └── expression_GSE9006.tsv        # Expression data
├── GSE10201/
│   ├── metadata_GSE10201.tsv
│   └── expression_GSE10201.tsv  
├── GSE15061/
│   ├── metadata_GSE15061.tsv
│   └── expression_GSE15061.tsv
└── GSE20142/
    ├── metadata_GSE20142.tsv
    └── expression_GSE20142.tsv
```

### Configuration File Structure  
```
/config/
├── config.yaml                      # Main configuration
├── filter_genes.tsv                 # Target gene list
└── field_mappings.yaml              # Metadata column mappings (optional)

/logs/
├── etl_20251014_102300.log          # Main processing log
├── data_quality_20251014.csv        # Quality summary report  
└── performance_20251014.json        # Performance metrics
```

### Sample File Formats

#### Filter Genes File (filter_genes.tsv)
```
gene_symbol	ensembl_id	refinebio_organism	gene_name
AAK1	ENSG00000115977	Homo sapiens	AP2 associated kinase 1
ACOT13	ENSG00000112304	Homo sapiens	acyl-CoA thioesterase 13
ADA2	ENSG00000093072	Homo sapiens	adenosine deaminase 2
```

#### Expression Data File (expression_GSE9006.tsv)  
```
Gene	GSM228562	GSM228563	GSM228564	GSM228565
ENSG00000115977	0.717	0.504	0.581	0.886
ENSG00000112304	0.562	0.596	0.628	0.739  
ENSG00000093072	0.429	0.466	0.533	0.337
```

#### Metadata File (metadata_GSE9006.tsv)
```
refinebio_accession_code	experiment_accession	refinebio_age	refinebio_sex	refinebio_platform	characteristics_ch1_Age	characteristics_ch1_Sex	characteristics_ch1_Illness
GSM228562	GSE9006	16	female	GPL96	16 yrs	F	Healthy
GSM228563	GSE9006		female	GPL96	16 yrs	F	Healthy  
GSM228564	GSE9006	25	male	GPL96	25 yrs	M	UNKNOWN
```

---

**Document Approval:**  
Requirements Analyst: [Signature]  
Date: October 14, 2025  

**Change Control:**  
Version 1.0 - Initial requirements document  
Next Review Date: November 14, 2025