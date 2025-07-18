import 'dotenv/config';
import { Client as Postgres } from 'pg';

import { MemFlow } from '../../../services/memflow';
import { WorkflowHandleService } from '../../../services/memflow/handle';
import { ClientService } from '../../../services/memflow/client';
import { guid } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ProvidersConfig } from '../../../types/provider';
import {
  dropTables,
  postgres_options,
} from '../../$setup/postgres';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = MemFlow;

describe('MEMFLOW | agent | `AI research agent with OpenAI integration` | Postgres', () => {
  const prefix = 'agent-';
  const namespace = 'prod';
  let client: ClientService;
  let workflowGuid: string;
  let postgresClient: ProviderNativeClient;
  let handle: WorkflowHandleService;
  
  beforeAll(async () => {
    if (process.env.POSTGRES_IS_REMOTE !== 'true') {
      postgresClient = (
        await PostgresConnection.connect(guid(), Postgres, postgres_options)
      ).getClient();

      await dropTables(postgresClient);
    }

    // Check if OpenAI API key is configured
    if (!process.env.OPENAI_API_KEY || process.env.OPENAI_API_KEY === 'your_openai_api_key_here') {
      console.warn('⚠️  OpenAI API key not configured. Tests will use mock responses.');
    }
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
          entity: 'research-agent',
          args: ['What are the long-term impacts of renewable energy subsidies?'],
          taskQueue: 'agents',
          workflowName: 'researchAgent',
          workflowId: workflowGuid,
          expire: 300, // 5 minutes for complex AI workflow
        });
        expect(handle.workflowId).toBeDefined();
      });
    });
  });

  describe('Worker', () => {
    describe('create', () => {

      it('should test basic functionality without OpenAI', async () => {
        // Create a worker for the basic test
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'agents',
          workflow: workflows.basicTest,
        });
        await worker.run();
    
        // Start the basic test workflow
        const testHandle = await client.workflow.start({
          namespace,
          entity: 'test-entity',
          args: ['BasicTest'],
          taskQueue: 'agents',
          workflowName: 'basicTest',
          workflowId: prefix + 'basic-test-' + guid(),
          expire: 30,
        });
    
        // Wait for the result
        const response = await testHandle.result();
        expect(response).toBeDefined();
        
        const result = response as any;
        expect(result.testName).toBe('BasicTest');
        expect(result.status).toBe('completed');
        expect(result.operations).toContain('test-started');
        expect(result.operations).toContain('test-completed');
        expect(result.completedAt).toBeDefined();
      });
    
      it('should create optimistic perspective hook worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'agents',
          workflow: workflows.optimisticPerspective,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create skeptical perspective hook worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'agents',
          workflow: workflows.skepticalPerspective,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create verification hook worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'agents',
          workflow: workflows.verificationHook,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create synthesis perspective worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'perspectives',
          workflow: workflows.synthesizePerspectives,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create main research agent worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'agents',
          workflow: workflows.researchAgent,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should return the complete research agent results with AI analysis', async () => {
        const response = await handle.result();
        expect(response).toBeDefined();
        expect(typeof response).toBe('object');
        
        const result = response as any;

        // Validate main response structure
        expect(result.query).toBe('What are the long-term impacts of renewable energy subsidies?');
        expect(result.status).toBe('synthesis-completed');
        expect(result.startTime).toBeDefined();
        
        // Validate perspectives structure
        expect(result.perspectives).toBeDefined();
        expect(result.perspectives.optimistic).toBeDefined();
        expect(result.perspectives.optimistic.findings).toBeInstanceOf(Array);
        expect(result.perspectives.optimistic.confidence).toBe(0.8);
        expect(result.perspectives.optimistic.completedAt).toBeDefined();

        expect(result.perspectives.skeptical).toBeDefined();
        expect(result.perspectives.skeptical.counterEvidence).toBeInstanceOf(Array);
        expect(result.perspectives.skeptical.confidence).toBe(0.6);
        expect(result.perspectives.skeptical.completedAt).toBeDefined();

        // Validate verification structure
        expect(result.verification).toBeDefined();
        expect(result.verification.credibility).toBeDefined();
        expect(result.verification.credibility.credibleSources).toBeInstanceOf(Array);
        expect(result.verification.credibility.questionableSources).toBeInstanceOf(Array);
        expect(result.verification.credibility.verificationMethod).toBeDefined();
        expect(result.verification.credibility.overallCredibility).toBeGreaterThan(0);

        // Validate synthesis structure
        expect(result.perspectives.synthesis).toBeDefined();
        expect(result.perspectives.synthesis.finalAssessment).toBeDefined();
        expect(result.perspectives.synthesis.finalAssessment.synthesis).toBeDefined();
        expect(result.perspectives.synthesis.finalAssessment.keyInsights).toBeInstanceOf(Array);
        expect(result.perspectives.synthesis.finalAssessment.recommendations).toBeInstanceOf(Array);
        expect(result.perspectives.synthesis.confidence).toBeGreaterThan(0);
        expect(result.perspectives.synthesis.completedAt).toBeDefined();
      }, 120_000); // 2 minutes timeout for OpenAI API calls
    });
  });
}); 