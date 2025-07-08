# MemFlow Agent Test Suite

This test suite demonstrates the **Recursive AI Research Agent** pattern using MemFlow's durable execution and permanent memory capabilities.

## Overview

The agent test showcases how MemFlow can be used to build intelligent, self-organizing workflows that:

1. **Use AI for Decision Making**: Leverage OpenAI (mocked in tests) to analyze questions and determine execution strategies
2. **Recursively Decompose Tasks**: Break complex questions into simpler sub-questions using child workflows
3. **Manage Shared Context**: Maintain evolving knowledge across all workflow generations
4. **Coordinate Multiple Hooks**: Execute specialized hooks (research, analysis, validation) based on AI recommendations
5. **Track Generational Progress**: Use semaphores to prevent infinite loops and manage recursion depth

## Test Architecture

### Core Components

- **`researchAgent`**: Main workflow that coordinates the entire research process
- **`researchHook`**: Gathers and validates information on specific topics
- **`analysisHook`**: Analyzes and synthesizes collected information
- **`decompositionHook`**: Breaks down complex questions and spawns child workflows
- **`validationHook`**: Validates findings and ensures data quality

### Activities (Mocked)

- **`openaiAnalyzer`**: Simulates OpenAI API calls for question analysis
- **`researchPlanner`**: Creates structured decomposition plans

## Use Case: Research Task Decomposition

The agent implements a **Research Task Decomposition System** that:

1. **Receives Complex Questions**: e.g., "What are the implications of artificial intelligence on future work?"
2. **AI Analysis**: Uses OpenAI to analyze complexity and recommend approach
3. **Dynamic Hook Execution**: Spawns appropriate hooks based on AI recommendations
4. **Recursive Decomposition**: If a question is too complex, it spawns child workflows for sub-questions
5. **Knowledge Aggregation**: All findings are stored in shared context accessible to all generations
6. **Intelligent Coordination**: Each generation can build upon previous knowledge

## Key Features Demonstrated

### 🤖 Agentic Control
- AI-driven decision making for task decomposition
- Dynamic hook selection based on question analysis
- Adaptive execution paths based on complexity assessment

### 🧠 Shared Contextual Updates
- Permanent memory that survives across all workflow generations
- Incremental knowledge building with each hook execution
- Cross-generational data sharing and learning

### 🔄 Recursive Execution
- Child workflows can spawn their own child workflows
- Generation tracking with depth limits to prevent infinite loops
- Hierarchical knowledge aggregation

### 🛡️ Safety Mechanisms
- Infinite loop protection with generation semaphores
- Maximum depth limits for recursive decomposition
- Error handling and graceful degradation

## Test Scenarios

1. **Basic Research Flow**: Simple questions that don't require decomposition
2. **Complex Research Flow**: Complex questions that trigger recursive decomposition
3. **Infinite Loop Protection**: Validates safety mechanisms work correctly
4. **Context Evolution**: Verifies that shared context evolves properly across executions

## Running the Tests

```bash
# Run the full agent test suite
npm test -- tests/memflow/agent/postgres.test.ts

# Run specific test cases
npm test -- tests/memflow/agent/postgres.test.ts -t "should test simple research agent"
```

## Architecture Benefits

This pattern is particularly powerful for:

- **Research and Analysis Tasks**: Breaking down complex research questions
- **Content Generation**: Multi-stage content creation with review and refinement
- **Data Processing Pipelines**: Intelligent data transformation with decision points
- **Customer Support**: Multi-step problem resolution with knowledge building
- **Educational Systems**: Adaptive learning paths based on student progress

The combination of durable execution, permanent memory, and AI-driven decision making creates a robust foundation for building sophisticated agent systems that can pause, resume, scale, and evolve over time.
