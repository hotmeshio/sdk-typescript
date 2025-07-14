# Document Processing Pipeline Test Suite

This test suite implements a **Document Processing Pipeline** use case that demonstrates how HotMesh can orchestrate AI-powered document processing workflows with permanent memory, hooks, signals, and retry mechanisms.

## Overview

The test suite demonstrates a complete implementation of a document processing pipeline featuring:

1. **PDF Document Processing**: Reads and processes PDF files page by page
2. **OpenAI Vision API Integration**: Uses actual OpenAI Vision API calls to extract member information
3. **Parallel Processing Hooks**: Implements multiple processing stages with hooks for validation, approval, and notification
4. **Entity State Management**: All data persists across workflow executions using permanent memory
5. **Signal-based Coordination**: Uses signals to coordinate between different processing stages
6. **Retry Mechanisms**: Demonstrates retry functionality with configurable retry counts
7. **Member Database Validation**: Validates extracted information against a JSON database

## Use Case: Membership Application Processing

The pipeline processes membership applications by:
- Reading PDF documents containing member information
- Extracting member ID and address information from each page using OpenAI Vision API
- Validating the extracted information against a member database
- Processing approval/rejection based on validation results
- Sending notifications about the processing outcome
- Handling retries for failed processing steps

## Setup

### Prerequisites

- PostgreSQL database
- OpenAI API key (GPT-4 Vision access recommended)
- Node.js environment with dotenv support
- pdf-parse package (automatically installed)

### Environment Configuration

Create a `.env` file in the project root:

```bash
# OpenAI API Configuration
OPENAI_API_KEY=your_openai_api_key_here
OPENAI_MODEL=gpt-4o-mini
```

## Test Structure

### Core Components

#### Main Workflow: `documentProcessingPipeline`
- Coordinates the entire document processing pipeline
- Reads PDF files and splits them into pages
- Launches parallel processing hooks for each stage
- Manages entity state and processing steps
- Handles error recovery and retry mechanisms

#### Processing Hooks
- **`pageProcessingHook`**: Processes individual PDF pages and extracts member information using OpenAI Vision API
- **`validationHook`**: Validates extracted member information against the member database
- **`approvalHook`**: Processes document approval/rejection based on validation results
- **`notificationHook`**: Sends notifications about processing outcomes
- **`retryProcessingHook`**: Handles retry logic for failed processing steps

#### Activities (Core Processing Logic)
- **`readPDFFile`**: Reads PDF files and extracts text content
- **`splitPDFIntoPages`**: Splits PDF content into individual pages
- **`extractMemberInfoFromPage`**: Uses OpenAI Vision API to extract member information from pages
- **`validateMemberInfo`**: Validates extracted information against member database
- **`processDocumentApproval`**: Processes approval workflow based on validation results
- **`sendNotification`**: Sends notifications about processing outcomes

### Signal-based Coordination

The pipeline uses signals to coordinate between processing stages:
- `pages-processed`: Signals when all pages have been processed
- `validation-complete`: Signals when validation is complete
- `approval-complete`: Signals when approval processing is complete
- `processing-complete`: Signals when the entire pipeline is complete

### Entity State Management

All processing state is maintained in permanent memory:
```typescript
{
  documentId: string;
  filePath: string;
  status: string;
  startTime: string;
  pages: string[];
  extractedInfo: any[];
  validationResults: any[];
  approvalResults: any[];
  notificationResults: any[];
  finalResult: any;
  processingSteps: string[];
  errors: any[];
}
```

## Test Files

### Core Test Files
- `postgres.test.ts`: Main test suite with comprehensive pipeline testing
- `src/workflows.ts`: Workflow definitions for all pipeline stages
- `src/activities.ts`: Activity implementations for PDF processing and OpenAI calls

### Configuration Files
- `fixtures/page#.png`: Sample image files for testing
- `fixtures/member-database.json`: Member database for validation testing

## Pipeline Demonstration Features

### 1. PDF Processing
- Reads PDF files using pdf-parse library
- Splits documents into individual pages
- Handles text extraction and page-by-page processing

### 2. OpenAI Vision API Integration
- Real OpenAI Vision API calls for document analysis
- Extracts member ID and address information
- Provides mock responses when API key is not configured

### 3. Parallel Processing
- Multiple hooks process different aspects simultaneously
- Page processing happens in parallel
- Validation, approval, and notification stages coordinate via signals

### 4. Entity State Persistence
- All processing state persists across workflow executions
- Shared memory between hooks enables coordinated processing
- Processing steps are tracked for audit and debugging

### 6. Member Database Validation
- JSON-based member database with validation rules
- Address matching and member status verification
- Configurable validation criteria

## Test Scenarios

### Basic Pipeline Test
Tests core pipeline functionality without OpenAI integration:
- Validates entity state management
- Confirms processing step tracking
- Verifies completion status

### Full Pipeline Test
Comprehensive test of the complete pipeline:
- PDF document processing
- OpenAI Vision API integration
- Member database validation
- Approval workflow processing
- Notification sending
- Final result compilation
