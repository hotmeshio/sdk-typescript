import { Client as Postgres } from 'pg';

import { MemFlow } from '../../../services/memflow';
import { WorkflowHandleService } from '../../../services/memflow/handle';
import { ClientService } from '../../../services/memflow/client';
import { WorkerService } from '../../../services/memflow/worker';
import { guid } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ProvidersConfig } from '../../../types/provider';
import {
  dropTables,
  postgres_options,
} from '../../$setup/postgres';

import * as workflows from './src/workflows';
import * as activities from './src/activities';

const { Connection, Client, Worker } = MemFlow;

describe('MEMFLOW | agent | `recursive AI research agent` | Postgres', () => {
  const prefix = 'agent-';
  const namespace = 'prod';
  let client: ClientService;
  let workflowGuid: string;
  let postgresClient: ProviderNativeClient;
  let handle: WorkflowHandleService;
  
  beforeAll(async () => {
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();

    await dropTables(postgresClient);
  });

  afterAll(async () => {
    await MemFlow.shutdown();
  }, 10_000);

  describe('Connection', () => {
    describe('connect', () => {
      it('should echo the Postgres config', async () => {
        const connection = (await Connection.connect({
          class: Postgres,
          options: postgres_options,
        })) as ProvidersConfig;
        expect(connection).toBeDefined();
      });
    });
  });

  describe('Client', () => {
    describe('start', () => {
      it('should connect a client and start a research agent workflow', async () => {
        client = new Client({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
        });

        workflowGuid = prefix + guid();

        handle = await client.workflow.start({
          namespace,
          args: ['What are the implications of artificial intelligence on future work?', 2],
          taskQueue: 'agent-queue',
          workflowName: 'researchAgent',
          workflowId: workflowGuid,
          expire: 180, // 3 minutes for the complex AI workflow
        });
        expect(handle.workflowId).toBeDefined();
      });
    });
  });

  describe('Worker', () => {
    describe('create', () => {
      it('should create main research agent worker', async () => {
        // Register activities before creating worker
        WorkerService.registerActivities(activities);
        
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'agent-queue',
          workflow: workflows.researchAgent,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create research hook worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'agent-queue',
          workflow: workflows.researchHook,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create analysis hook worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'agent-queue',
          workflow: workflows.analysisHook,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create decomposition hook worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'agent-queue',
          workflow: workflows.decompositionHook,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create validation hook worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'agent-queue',
          workflow: workflows.validationHook,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should test infinite loop protection in agent context', async () => {
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'agent-queue',
          workflow: workflows.testInfiniteLoopProtection,
        });
        await worker.run();

        // Start the test workflow
        const testHandle = await client.workflow.start({
          namespace,
          args: ['Test question for infinite loop protection'],
          taskQueue: 'agent-queue',
          workflowName: 'testInfiniteLoopProtection',
          workflowId: prefix + 'infinite-loop-test-' + guid(),
          expire: 30,
        });

        // Wait for the result
        const response = await testHandle.result();
        expect(response).toBeDefined();
        
        const result = response as any;
        expect(result.success).toBe(true);
        expect(result.message).toBe('Infinite loop protection working correctly');
        expect(result.error).toContain('MemFlow Hook Error: Potential infinite loop detected!');
      });

      it('should test simple research agent without recursion', async () => {
        // Create a simple test workflow
        const testHandle = await client.workflow.start({
          namespace,
          args: ['Simple research question about renewable energy?', 1], // Max depth 1 to prevent recursion
          taskQueue: 'agent-queue',
          workflowName: 'researchAgent',
          workflowId: prefix + 'simple-research-' + guid(),
          expire: 60,
        });

        // Wait for the result
        const response = await testHandle.result();
        expect(response).toBeDefined();
        
        const result = response as any;
        
        // Validate basic response structure
        expect(result).toHaveProperty('success', true);
        expect(result).toHaveProperty('message');
        expect(result.message).toContain('Research agent completed analysis');
        expect(result.message).toContain('renewable energy');
        
        // Validate AI analysis results
        expect(result).toHaveProperty('analysis');
        expect(result.analysis).toHaveProperty('taskType');
        expect(result.analysis).toHaveProperty('complexity');
        expect(result.analysis).toHaveProperty('nextActions');
        expect(result.analysis.nextActions).toContain('research-hook');
        
        // Validate execution plan
        expect(result).toHaveProperty('plan');
        expect(result.plan).toHaveProperty('question');
        expect(result.plan).toHaveProperty('complexity');
        expect(result.plan).toHaveProperty('hooks');
        
        // Validate hook execution
        expect(result).toHaveProperty('hookResults');
        expect(Array.isArray(result.hookResults)).toBe(true);
        expect(result.hookResults.length).toBeGreaterThan(0);
        
        // Validate final context
        expect(result).toHaveProperty('finalContext');
        expect(result.finalContext).toHaveProperty('originalQuestion');
        expect(result.finalContext).toHaveProperty('generation', 0);
        expect(result.finalContext).toHaveProperty('status', 'hooks-completed');
        expect(result.finalContext).toHaveProperty('metrics');
        expect(result.finalContext.metrics).toHaveProperty('totalHooks');
        expect(result.finalContext.metrics).toHaveProperty('totalActivities');
        
        // Validate summary
        expect(result).toHaveProperty('summary');
        expect(result.summary).toHaveProperty('totalHooks');
        expect(result.summary).toHaveProperty('complexity');
        expect(result.summary).toHaveProperty('generation', 0);
      });

      it('should return the complete research agent results with context evolution', async () => {
        const response = await handle.result();
        expect(response).toBeDefined();
        expect(typeof response).toBe('object');
        
        const result = response as any;

        // Validate main response structure
        expect(result).toHaveProperty('success', true);
        expect(result).toHaveProperty('message');
        expect(result.message).toContain('Research agent completed analysis');
        expect(result.message).toContain('artificial intelligence');
        
        // Validate AI analysis
        expect(result).toHaveProperty('analysis');
        expect(result.analysis).toHaveProperty('taskType');
        expect(result.analysis).toHaveProperty('complexity');
        expect(result.analysis).toHaveProperty('nextActions');
        expect(result.analysis).toHaveProperty('generation', 0);
        expect(result.analysis).toHaveProperty('confidence');
        expect(result.analysis.confidence).toBeGreaterThan(0.5);
        
        // Validate planning results
        expect(result).toHaveProperty('plan');
        expect(result.plan).toHaveProperty('question');
        expect(result.plan).toHaveProperty('complexity');
        expect(result.plan).toHaveProperty('decomposition');
        expect(result.plan).toHaveProperty('hooks');
        expect(result.plan).toHaveProperty('estimatedDepth');
        
        // Validate hook execution results
        expect(result).toHaveProperty('hookResults');
        expect(Array.isArray(result.hookResults)).toBe(true);
        expect(result.hookResults.length).toBeGreaterThan(0);
        
        // Validate at least one hook result
        const firstHookResult = result.hookResults[0];
        expect(firstHookResult).toBeDefined();
        expect(firstHookResult).toHaveProperty('hookType');
        expect(firstHookResult).toHaveProperty('status');
        expect(firstHookResult).toHaveProperty('generation');
        
        // Validate final context structure
        expect(result).toHaveProperty('finalContext');
        expect(result.finalContext).toHaveProperty('originalQuestion');
        expect(result.finalContext.originalQuestion).toContain('artificial intelligence');
        expect(result.finalContext).toHaveProperty('generation', 0);
        expect(result.finalContext).toHaveProperty('maxDepth', 2);
        expect(result.finalContext).toHaveProperty('status', 'hooks-completed');
        expect(result.finalContext).toHaveProperty('operations');
        expect(result.finalContext).toHaveProperty('metrics');
        
        // Validate context operations
        expect(Array.isArray(result.finalContext.operations)).toBe(true);
        expect(result.finalContext.operations).toContain('ai-analysis-completed');
        expect(result.finalContext.operations).toContain('plan-created');
        expect(result.finalContext.operations).toContain('hooks-executed');
        
        // Validate context metrics
        expect(result.finalContext.metrics).toHaveProperty('totalHooks');
        expect(result.finalContext.metrics).toHaveProperty('totalGenerations');
        expect(result.finalContext.metrics).toHaveProperty('totalActivities');
        expect(result.finalContext.metrics).toHaveProperty('completedTasks');
        expect(result.finalContext.metrics.totalHooks).toBeGreaterThan(0);
        expect(result.finalContext.metrics.totalActivities).toBeGreaterThan(0);
        expect(result.finalContext.metrics.completedTasks).toBeGreaterThan(0);
        
        // Validate summary
        expect(result).toHaveProperty('summary');
        expect(result.summary).toHaveProperty('totalHooks');
        expect(result.summary).toHaveProperty('complexity');
        expect(result.summary).toHaveProperty('nextActions');
        expect(result.summary).toHaveProperty('generation', 0);
        expect(result.summary.totalHooks).toBe(result.hookResults.length);
        
        // Validate that analysis and plan are consistent
        expect(result.analysis.complexity).toBe(result.plan.complexity);
        expect(result.summary.complexity).toBe(result.analysis.complexity);
        expect(result.summary.nextActions).toEqual(result.analysis.nextActions);
        
        // Validate that context contains research data if research hook was executed
        if (result.analysis.nextActions.includes('research-hook')) {
          expect(result.finalContext).toHaveProperty('research_0');
          expect(result.finalContext.research_0).toHaveProperty('findings');
          expect(result.finalContext.research_0).toHaveProperty('sources');
          expect(result.finalContext.research_0).toHaveProperty('confidence');
        }
        
        // Validate that context contains analysis data if analysis hook was executed
        if (result.analysis.nextActions.includes('analysis-hook')) {
          expect(result.finalContext).toHaveProperty('analysis_0');
          expect(result.finalContext.analysis_0).toHaveProperty('patterns');
          expect(result.finalContext.analysis_0).toHaveProperty('insights');
          expect(result.finalContext.analysis_0).toHaveProperty('recommendations');
        }
        
        // If decomposition hook was executed, validate child workflows
        if (result.analysis.nextActions.includes('decomposition-hook')) {
          expect(result.finalContext).toHaveProperty('childWorkflows');
          expect(Array.isArray(result.finalContext.childWorkflows)).toBe(true);
          // Note: childWorkflows may be empty if max depth was reached
        }
        
        console.log('✅ Research Agent Test Results:');
        console.log(`   Question: "${result.finalContext.originalQuestion}"`);
        console.log(`   Complexity: ${result.analysis.complexity}`);
        console.log(`   Total Hooks: ${result.summary.totalHooks}`);
        console.log(`   Total Activities: ${result.finalContext.metrics.totalActivities}`);
        console.log(`   Operations: ${result.finalContext.operations.join(', ')}`);
        console.log(`   Next Actions: ${result.analysis.nextActions.join(', ')}`);
        
      }, 60_000); // 60 seconds timeout for complex AI workflow
    });
  });
});
