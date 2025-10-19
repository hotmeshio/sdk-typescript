import { MemFlow } from '../../../../services/memflow';
import { 
  searchForSupportingEvidence, 
  searchForCounterEvidence, 
  analyzePerspectives, 
  verifySourceCredibility 
} from './activities';

// Set up proxy activities for OpenAI calls
const activities = MemFlow.workflow.proxyActivities<{
  searchForSupportingEvidence: typeof searchForSupportingEvidence;
  searchForCounterEvidence: typeof searchForCounterEvidence;
  analyzePerspectives: typeof analyzePerspectives;
  verifySourceCredibility: typeof verifySourceCredibility;
}>({
  activities: { 
    searchForSupportingEvidence, 
    searchForCounterEvidence, 
    analyzePerspectives, 
    verifySourceCredibility 
  },
  retryPolicy: {
    maximumAttempts: 3,
    backoffCoefficient: 2,
    maximumInterval: '10 seconds',
    throwOnError: true
  }
});

/**
 * Main Research Agent Workflow
 * Coordinates AI-driven research with multiple perspectives and child workflows
 * Based on the README example
 */
export async function researchAgent(query: string): Promise<any> {
  const agent = await MemFlow.workflow.entity();

  // Set up shared memory for this agent session
  const initialState = {
    query,
    findings: [],
    perspectives: {},
    confidence: 0,
    verification: {},
    status: 'researching',
    startTime: new Date().toISOString(),
  }
  await agent.set<typeof initialState>(initialState);

  // Run independent research perspectives in parallel using batch execution
  await MemFlow.workflow.execHookBatch([
    {
      key: 'optimistic',
      options: {
        taskQueue: 'agents',
        workflowName: 'optimisticPerspective',
        args: [query],
        signalId: 'optimistic-complete'
      }
    },
    {
      key: 'skeptical',
      options: {
        taskQueue: 'agents',
        workflowName: 'skepticalPerspective',
        args: [query],
        signalId: 'skeptical-complete'
      }
    }
  ]);

  // Verify sources after research completes
  await MemFlow.workflow.execHook({
    taskQueue: 'agents',
    workflowName: 'verificationHook',
    args: [query],
    signalId: 'verification-complete'
  });

  await MemFlow.workflow.execHook({
    taskQueue: 'perspectives',
    workflowName: 'synthesizePerspectives',
    args: [],
    signalId: 'synthesis-complete',
  });

  // return analysis, verification, and synthesis
  return await agent.get();
}

/**
 * Optimistic Perspective Hook
 * Looks for affirming evidence using OpenAI
 */
export async function optimisticPerspective(query: string): Promise<void> {
  const entity = await MemFlow.workflow.entity();

  // Search for supporting evidence using OpenAI
  const findings = await activities.searchForSupportingEvidence(query);
  
  await entity.merge({ 
    perspectives: { 
      optimistic: { 
        findings, 
        confidence: 0.8,
        completedAt: new Date().toISOString()
      }
    }
  });

  // Signal completion //memory is shared, so entity already updated
  await MemFlow.workflow.signal('optimistic-complete', {});
}

/**
 * Skeptical Perspective Hook
 * Seeks out contradictions and counterpoints using OpenAI
 */
export async function skepticalPerspective(query: string): Promise<void> {
  const entity = await MemFlow.workflow.entity();

  // Search for counter evidence using OpenAI
  const counterEvidence = await activities.searchForCounterEvidence(query);
  
  await entity.merge({ 
    perspectives: { 
      skeptical: { 
        counterEvidence, 
        confidence: 0.6,
        completedAt: new Date().toISOString()
      }
    }
  });

  // Signal completion //memory is shared, so entity already updated
  await MemFlow.workflow.signal('skeptical-complete', {});
}

/**
 * Verification Hook
 * Verifies source credibility and merges into shared agent state
 */
export async function verificationHook(query: string): Promise<void> {
  const entity = await MemFlow.workflow.entity();

  // Verify source credibility using OpenAI
  const verification = await activities.verifySourceCredibility(query);
  
  await entity.merge({ 
    verification: {
      credibility: verification,
      completedAt: new Date().toISOString()
    }
  });

  // Signal completion //memory is shared, so entity already updated
  await MemFlow.workflow.signal('verification-complete', {});
}

/**
 * Synthesis Hook
 * Aggregates different viewpoints using OpenAI analysis
 */
export async function synthesizePerspectives(): Promise<void> {
  const entity = await MemFlow.workflow.entity();
  const perspectives = await entity.get();

  // Analyze all perspectives using OpenAI
  const result = await activities.analyzePerspectives(perspectives);

  const merged = await entity.merge({
    perspectives: {
      synthesis: {
        finalAssessment: result,
        confidence: result.confidence || 0.75,
        completedAt: new Date().toISOString()
      }
    },
    status: 'synthesis-completed',
  });

  // Signal completion
  await MemFlow.workflow.signal('synthesis-complete', {});
}

/**
 * Test workflow for basic functionality
 */
export async function basicTest(name: string): Promise<any> {
  const entity = await MemFlow.workflow.entity();
  
  await entity.set({
    testName: name,
    startTime: new Date().toISOString(),
    operations: []
  });

  // Simple test operation
  await entity.append('operations', 'test-started');
  await entity.merge({ status: 'testing' });

  // Wait a moment
  await MemFlow.workflow.sleepFor('1 second');

  await entity.append('operations', 'test-completed');
  await entity.merge({ 
    status: 'completed',
    completedAt: new Date().toISOString()
  });

  return await entity.get();
}
