import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import 'dotenv/config';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { ClientService } from '../../../services/durable/client';
import { guid } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ProvidersConfig } from '../../../types/provider';
import {
  dropTables,
  postgres_options,
} from '../../$setup/postgres';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = Durable;

describe('DURABLE | pipeline | `Document Processing Pipeline with OpenAI Vision` | Postgres', () => {
  const prefix = 'pipeline-';
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
    await Durable.shutdown();
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
      it('should connect a client and start a document processing pipeline', async () => {
        client = new Client({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
        });

        workflowGuid = prefix + guid();

        handle = await client.workflow.start({
          namespace,
          entity: 'document-processor',
          args: [],
          taskQueue: 'pipeline',
          workflowName: 'documentProcessingPipeline',
          workflowId: workflowGuid,
          expire: 120, // 2 minutes for document processing
        });
        expect(handle.workflowId).toBeDefined();
      });
    });
  });

  describe('Worker', () => {
    describe('create', () => {

      it('should test basic pipeline functionality without OpenAI', async () => {
        // Create a worker for the basic test
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'pipeline',
          workflow: workflows.basicPipelineTest,
        });
        await worker.run();
    
        // Start the basic test workflow
        const testHandle = await client.workflow.start({
          namespace,
          entity: 'test-entity',
          args: ['BasicPipelineTest'],
          taskQueue: 'pipeline',
          workflowName: 'basicPipelineTest',
          workflowId: prefix + 'basic-test-' + guid(),
          expire: 30,
        });
    
        // Wait for the result
        const response = await testHandle.result();
        expect(response).toBeDefined();
        
        const result = response as any;
        expect(result.testName).toBe('BasicPipelineTest');
        expect(result.status).toBe('completed');
        expect(result.operations).toContain('test-started');
        expect(result.operations).toContain('test-completed');
        expect(result.completedAt).toBeDefined();
      });
    
      it('should create page processing hook worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'pipeline',
          workflow: workflows.pageProcessingHook,
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
          taskQueue: 'pipeline',
          workflow: workflows.validationHook,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create approval hook worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'pipeline',
          workflow: workflows.approvalHook,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create notification hook worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'pipeline',
          workflow: workflows.notificationHook,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create main document processing pipeline worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'pipeline',
          workflow: workflows.documentProcessingPipeline,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should return the complete document processing pipeline results', async () => {
        const response = await handle.result();
        expect(response).toBeDefined();
        expect(typeof response).toBe('object');
        
        const result = response as any;

        // Validate main response structure
        expect(result.documentId).toBeDefined();
        expect(result.documentId).toContain('doc-');
        expect(result.status).toBe('completed');
        expect(result.startTime).toBeDefined();
        expect(result.completedAt).toBeDefined();
        
        // Validate processing steps
        expect(result.processingSteps).toBeDefined();
        expect(result.processingSteps).toBeInstanceOf(Array);
        expect(result.processingSteps).toEqual([
          'image-load-started',
          'image-load-completed',
          'page-1-processed',
          'page-2-processed',
          'validation-completed',
          'approval-completed',
          'notifications-sent',
          'notifications-completed',
          'pipeline-completed',
        ]);

        // Validate empty errors array
        expect(result.errors).toBeDefined();
        expect(result.errors).toBeInstanceOf(Array);
        expect(result.errors.length).toBe(0);

        // Validate image references
        expect(result.imageRefs).toBeDefined();
        expect(result.imageRefs).toBeInstanceOf(Array);
        expect(result.imageRefs).toContain('page1.png');
        expect(result.imageRefs).toContain('page2.png');

        // Validate timestamps
        expect(result.startTime).toBeDefined();
        expect(new Date(result.startTime).toISOString()).toBe(result.startTime); // Valid ISO string
        expect(result.completedAt).toBeDefined();
        expect(new Date(result.completedAt).toISOString()).toBe(result.completedAt); // Valid ISO string

        // Validate document ID format
        expect(result.documentId).toBeDefined();
        expect(result.documentId).toMatch(/^doc-\d+$/);

        // Validate final result structure
        expect(result.finalResult).toBeDefined();
        expect(result.finalResult.status).toBe('completed');
        expect(result.finalResult.documentId).toMatch(/^doc-\d+$/);
        expect(result.finalResult.totalPages).toBe(2);
        expect(result.finalResult.notifications).toBe(1);
        expect(typeof result.finalResult.processingTime).toBe('number');
        expect(result.finalResult.approvedMembers).toBe(0);
        expect(result.finalResult.extractedMembers).toBe(2);
        expect(result.finalResult.validatedMembers).toBe(0);

        // Validate page signals
        expect(result.pageSignals).toBeDefined();
        expect(result.pageSignals['page-1-complete']).toBe(false);
        expect(result.pageSignals['page-2-complete']).toBe(false);

        // Validate extracted info array
        expect(result.extractedInfo).toBeDefined();
        expect(result.extractedInfo).toBeInstanceOf(Array);
        expect(result.extractedInfo.length).toBeGreaterThan(0);
        const firstExtractedInfo = result.extractedInfo[0];
        expect(firstExtractedInfo.imageRef).toBe('page1.png');
        expect(firstExtractedInfo.pageNumber).toBe(1);
        expect(firstExtractedInfo.extractedAt).toBeDefined();
        expect(firstExtractedInfo.memberInfo).toBeDefined();
        expect(firstExtractedInfo.memberInfo.name).toBe('John Smith');
        expect(firstExtractedInfo.memberInfo.email).toBe('john.smith@example.org');
        expect(firstExtractedInfo.memberInfo.phone).toBe('(310) 555-9012');
        expect(firstExtractedInfo.memberInfo.memberId).toBe('MBR-2024-001');
        expect(firstExtractedInfo.memberInfo.address).toBeDefined();
        expect(firstExtractedInfo.memberInfo.address.street).toBe('456 Elm Street');
        expect(firstExtractedInfo.memberInfo.address.city).toBe('Rivertown');
        expect(firstExtractedInfo.memberInfo.address.state).toBe('CA');
        expect(firstExtractedInfo.memberInfo.address.zip).toBe('90210');

        // Validate approval results array
        expect(result.approvalResults).toBeDefined();
        expect(result.approvalResults).toBeInstanceOf(Array);
        expect(result.approvalResults.length).toBeGreaterThan(0);
        const firstApprovalResult = result.approvalResults[0];
        expect(firstApprovalResult.pages).toEqual([1, 2]);
        expect(firstApprovalResult.memberId).toBe('MBR-2024-001');
        expect(firstApprovalResult.processedAt).toBeDefined();
        expect(firstApprovalResult.approvalResult).toBeDefined();
        expect(firstApprovalResult.approvalResult.status).toBe('rejected');
        expect(firstApprovalResult.approvalResult.memberId).toBe('MBR-2024-001');
        expect(firstApprovalResult.approvalResult.memberType).toBe('premium');
        expect(firstApprovalResult.approvalResult.validationResult).toBe(false);
        expect(firstApprovalResult.approvalResult.nextSteps).toContain('Send rejection notice');
        expect(firstApprovalResult.approvalResult.nextSteps).toContain('Request updated information');

        // Validate validation results array
        expect(result.validationResults).toBeDefined();
        expect(result.validationResults).toBeInstanceOf(Array);
        expect(result.validationResults.length).toBeGreaterThan(0);
        const firstValidationResult = result.validationResults[0];
        expect(firstValidationResult.pages).toEqual([1, 2]);
        expect(firstValidationResult.isValid).toBe(false);
        expect(firstValidationResult.memberId).toBe('MBR-2024-001');
        expect(firstValidationResult.validatedAt).toBeDefined();
        expect(firstValidationResult.combinedInfo).toBeDefined();
        expect(firstValidationResult.combinedInfo.name).toBe('John Smith');
        expect(firstValidationResult.combinedInfo.email).toBe('john.smith@example.org');
        expect(firstValidationResult.combinedInfo.phone).toBe('(310) 555-9012');
        expect(firstValidationResult.combinedInfo.memberId).toBe('MBR-2024-001');
        expect(firstValidationResult.combinedInfo.emergencyContact).toBeDefined();
        expect(firstValidationResult.combinedInfo.emergencyContact.name).toBe('Emily Smith');
        expect(firstValidationResult.combinedInfo.emergencyContact.phone).toBe('(310) 555-7721');

        // Validate notification results array
        expect(result.notificationResults).toBeDefined();
        expect(result.notificationResults).toBeInstanceOf(Array);
        expect(result.notificationResults.length).toBeGreaterThan(0);
        const firstNotificationResult = result.notificationResults[0];
        expect(firstNotificationResult.status).toBe('rejected');
        expect(firstNotificationResult.memberId).toBe('MBR-2024-001');
        expect(firstNotificationResult.notifiedAt).toBeDefined();
        
      }, 15_000);
    });
  });
}); 