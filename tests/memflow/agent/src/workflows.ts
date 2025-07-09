import { MemFlow } from '../../../../services/memflow';
import { openaiAnalyzer, researchPlanner } from './activities';

/**
 * Main Research Agent Workflow
 * Coordinates AI-driven research task decomposition and execution
 */
export async function researchAgent(question: string, maxDepth: number = 3): Promise<any> {
  const entity = await MemFlow.workflow.entity();
  
  // Initialize the research entity
  await entity.set({
    originalQuestion: question,
    generation: 0,
    maxDepth,
    startTime: new Date().toISOString(),
    status: 'initialized',
    findings: [],
    operations: [],
    metrics: {
      totalHooks: 0,
      totalGenerations: 0,
      totalActivities: 0,
      completedTasks: 0
    },
    childWorkflows: [],
    knowledge: {}
  });

  // Set up proxy activities for AI calls
  const activities = MemFlow.workflow.proxyActivities<{
    openaiAnalyzer: typeof openaiAnalyzer;
    researchPlanner: typeof researchPlanner;
  }>({
    activities: { openaiAnalyzer, researchPlanner }
  });

  // Step 1: Analyze the question with AI
  const analysis = await activities.openaiAnalyzer(
    `Analyze this research question: "${question}". Determine complexity and recommend approach.`,
    await entity.get(),
    0
  );

  // Update entity with analysis
  await entity.merge({
    analysis,
    status: 'analyzed',
    metrics: { totalActivities: 1 }
  });

  // Step 2: Create execution plan
  const plan = await activities.researchPlanner(question, await entity.get(), 0);
  
  await entity.merge({
    plan,
    status: 'planned'
  });

  // Step 3: Execute hooks based on AI recommendations
  const hookPromises: Promise<any>[] = [];
  
  for (const actionType of analysis.nextActions) {
    switch (actionType) {
      case 'research-hook':
        hookPromises.push(
          MemFlow.workflow.execHook({
            taskQueue: 'agent-queue',
            workflowName: 'researchHook',
            args: [question, 'research', 0],
            signalId: `research-complete-${Math.random().toString(36).substr(2, 9)}`
          })
        );
        break;
      
      case 'analysis-hook':
        hookPromises.push(
          MemFlow.workflow.execHook({
            taskQueue: 'agent-queue',
            workflowName: 'analysisHook',
            args: [question, 'analysis', 0],
            signalId: `analysis-complete-${Math.random().toString(36).substr(2, 9)}`
          })
        );
        break;
      
      case 'decomposition-hook':
        hookPromises.push(
          MemFlow.workflow.execHook({
            taskQueue: 'agent-queue',
            workflowName: 'decompositionHook',
            args: [question, maxDepth, 0],
            signalId: `decomposition-complete-${Math.random().toString(36).substr(2, 9)}`
          })
        );
        break;
    }
  }

  // Wait for all hooks to complete
  const hookResults = await Promise.all(hookPromises);
  
  // Update entity with hook results
  await entity.merge({
    hookResults,
    status: 'hooks-completed',
    metrics: { totalHooks: hookResults.length }
  });

  // Append operations
  await entity.append('operations', 'ai-analysis-completed');
  await entity.append('operations', 'plan-created');
  await entity.append('operations', 'hooks-executed');
  
  // Increment metrics
  await entity.increment('metrics.completedTasks', hookResults.length);
  await entity.increment('metrics.totalGenerations', 1);

  // Get final entity
  const finalEntity = await entity.get();
  
  return {
    success: true,
    message: `Research agent completed analysis of: "${question}"`,
    analysis,
    plan,
    hookResults,
    finalEntity,
    summary: {
      totalHooks: hookResults.length,
      complexity: analysis.complexity,
      nextActions: analysis.nextActions,
      generation: 0
    }
  };
}

/**
 * Research Hook - Gathers and validates information
 */
export async function researchHook(question: string, hookType: string, generation: number): Promise<any> {
  const entity = await MemFlow.workflow.entity();
  
  // Simulate research processing
  await MemFlow.workflow.sleepFor('1 second');
  
  // Create mock research findings
  const findings = [
    `Research finding 1 for: ${question}`,
    `Research finding 2 for: ${question}`,
    `Validated source information for: ${question}`
  ];
  
  // Update shared entity with findings
  await entity.merge({
    [`research_${generation}`]: {
      findings,
      sources: ['Source A', 'Source B', 'Source C'],
      confidence: 0.85,
      timestamp: new Date().toISOString()
    }
  });
  
  const result = {
    hookType,
    question,
    generation,
    findings,
    status: 'completed',
    processingTime: 1000,
    confidence: 0.85
  };
  
  // Signal completion
  await MemFlow.workflow.signal(`research-complete-${generation}`, result);
  
  return result;
}

/**
 * Analysis Hook - Analyzes and synthesizes information
 */
export async function analysisHook(question: string, hookType: string, generation: number): Promise<any> {
  const entity = await MemFlow.workflow.entity();
  
  // Simulate analysis processing
  await MemFlow.workflow.sleepFor('1.5 seconds');
  
  // Get current entity to analyze existing data
  const currentEntity = await entity.get();
  
  // Create analysis based on existing entity
  const analysis = {
    patterns: ['Pattern A identified', 'Pattern B identified'],
    insights: [`Key insight about: ${question}`, 'Cross-reference insight'],
    recommendations: ['Recommendation 1', 'Recommendation 2'],
    confidence: 0.90
  };
  
  // Update entity with analysis
  await entity.merge({
    [`analysis_${generation}`]: {
      ...analysis,
      timestamp: new Date().toISOString(),
      basedOn: Object.keys(currentEntity.knowledge || {})
    }
  });
  
  const result = {
    hookType,
    question,
    generation,
    analysis,
    status: 'completed',
    processingTime: 1500,
    confidence: 0.90
  };
  
  return result;
}

/**
 * Decomposition Hook - Breaks down complex questions and spawns child workflows
 */
export async function decompositionHook(question: string, maxDepth: number, generation: number): Promise<any> {
  const entity = await MemFlow.workflow.entity();
  
  // Check if we've reached max depth
  if (generation >= maxDepth) {
    return {
      hookType: 'decomposition',
      question,
      generation,
      status: 'max-depth-reached',
      message: 'Maximum decomposition depth reached',
      childWorkflows: []
    };
  }
  
  // Simulate decomposition processing
  await MemFlow.workflow.sleepFor('2 seconds');
  
  // Break down the question into sub-questions
  const subQuestions = [
    `Sub-question 1: ${question.substring(0, 20)}...?`,
    `Sub-question 2: What are the implications of ${question}?`,
    `Sub-question 3: How does this relate to broader entity?`
  ];
  
  const childWorkflows: any[] = [];
  
  // Spawn child workflows for each sub-question (recursive)
  for (let i = 0; i < subQuestions.length; i++) {
    const subQuestion = subQuestions[i];
    const childWorkflowId = `child-${generation + 1}-${i}-${Date.now()}`;
    
    try {
      const childResult = await MemFlow.workflow.executeChild({
        taskQueue: 'agent-queue',
        workflowName: 'researchAgent',
        args: [subQuestion, maxDepth],
        workflowId: childWorkflowId
      });
      
      childWorkflows.push({
        workflowId: childWorkflowId,
        question: subQuestion,
        generation: generation + 1,
        result: childResult,
        status: 'completed'
      });
      
      // Update entity with child results
      await entity.merge({
        [`child_${generation + 1}_${i}`]: childResult
      });
      
    } catch (error) {
      childWorkflows.push({
        workflowId: childWorkflowId,
        question: subQuestion,
        generation: generation + 1,
        error: error.message,
        status: 'failed'
      });
    }
  }
  
  // Update generation counter
  await entity.increment('metrics.totalGenerations', childWorkflows.length);
  
  // Update child workflows list
  for (const childWorkflow of childWorkflows) {
    await entity.append('childWorkflows', childWorkflow);
  }
  
  const result = {
    hookType: 'decomposition',
    question,
    generation,
    subQuestions,
    childWorkflows,
    status: 'completed',
    processingTime: 2000,
    nextGeneration: generation + 1
  };
  
  return result;
}

/**
 * Test workflow for validation hook functionality
 */
export async function validationHook(question: string, hookType: string, generation: number): Promise<any> {
  const entity = await MemFlow.workflow.entity();
  
  // Simulate validation processing
  await MemFlow.workflow.sleepFor('800ms');
  
  // Get current entity to validate against
  const currentEntity = await entity.get();
  
  // Create validation results
  const validation = {
    isValid: true,
    confidence: 0.95,
    checkedItems: ['Fact 1', 'Fact 2', 'Source validity'],
    warnings: [],
    errors: []
  };
  
  // Update entity with validation
  await entity.merge({
    [`validation_${generation}`]: {
      ...validation,
      timestamp: new Date().toISOString(),
      validatedEntity: Object.keys(currentEntity)
    }
  });
  
  const result = {
    hookType,
    question,
    generation,
    validation,
    status: 'completed',
    processingTime: 800
  };
  
  return result;
}

/**
 * Test workflow for infinite loop protection
 */
export async function testInfiniteLoopProtection(question: string): Promise<any> {
  try {
    // This should throw an error due to missing taskQueue
    await MemFlow.workflow.hook({
      workflowName: 'researchHook',
      args: [question, 'test', 0]
    });
    
    return { error: 'Expected error was not thrown!' };
  } catch (error) {
    return { 
      success: true, 
      message: 'Infinite loop protection working correctly',
      error: error.message 
    };
  }
}
