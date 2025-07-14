# AI Research Agent Test Suite

This test suite implements the **Research Agent** use case from the main README, demonstrating how HotMesh can orchestrate AI-powered workflows with permanent memory and multi-perspective analysis.

## Overview

The test suite demonstrates a complete implementation of the research agent pattern from the README, featuring:

1. **Real OpenAI API Integration**: Uses actual GPT-4.1 API calls for research and analysis
2. **Multiple Perspectives**: Implements validation, optimistic, and skeptical perspective hooks
4. **Synthesis**: Aggregates and analyzes different perspectives using AI
5. **Permanent Memory**: All data persists across workflow executions

## Setup

### Prerequisites

- PostgreSQL database
- OpenAI API key (GPT-4.1 access)
- Node.js environment with dotenv support

### Environment Configuration

Create a `.env` file in the project root:

```bash
# OpenAI API Configuration
OPENAI_API_KEY=your_openai_api_key_here
```

## Test Structure

### Core Components

#### Main Workflow: `researchAgent`
- Coordinates the entire research process
- Launches parallel perspective and verification hooks
- Manages entity state and operations
- All hooks share the same entity memory

#### Perspective Hooks
- **`optimist`**: Searches for supporting evidence using OpenAI
- **`skeptic`**: Finds counter-evidence and contradictory information
- **`synthesizer`**: Analyzes and synthesizes all perspectives
- **`verifyer`**: Verifies source credibility using AI and merges into shared state

#### Activities (OpenAI Integration)
- **`searchForSupportingEvidence`**: Finds evidence supporting a query
- **`searchForCounterEvidence`**: Finds contradictory information
- **`analyzePerspectives`**: Synthesizes multiple viewpoints
- **`verifySourceCredibility`**: Evaluates source reliability

## Running the Tests

### Full Test Suite
```bash
npm run test:memflow:agent
```

### Individual Test Components
```bash
# Test basic functionality (no OpenAI required)
NODE_ENV=test jest tests/memflow/agent/postgres.test.ts -t "basic functionality"

# Test with OpenAI integration
NODE_ENV=test jest tests/memflow/agent/postgres.test.ts -t "complete research agent"
```

## Mock Mode

If no OpenAI API key is configured, the activities will fall back to mock responses, allowing the test to verify workflow orchestration without external API dependencies.
