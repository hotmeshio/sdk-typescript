import { MemFlow } from '../../../../services/memflow';
import { 
  loadImagePages,
  extractMemberInfoFromPage,
  validateMemberInfo,
  processDocumentApproval,
  sendNotification
} from './activities';

// Set up proxy activities for PNG image processing and OpenAI calls
const activities = MemFlow.workflow.proxyActivities<{
  loadImagePages: typeof loadImagePages;
  extractMemberInfoFromPage: typeof extractMemberInfoFromPage;
  validateMemberInfo: typeof validateMemberInfo;
  processDocumentApproval: typeof processDocumentApproval;
  sendNotification: typeof sendNotification;
}>({
  activities: { 
    loadImagePages,
    extractMemberInfoFromPage,
    validateMemberInfo,
    processDocumentApproval,
    sendNotification
  },
  retryPolicy: {
    maximumAttempts: 2,
    backoffCoefficient: 2,
    maximumInterval: '5 seconds',
    throwOnError: true
  }
});

/**
 * Main Document Processing Pipeline Workflow
 * Coordinates document processing with hooks for different stages
 */
export async function documentProcessingPipeline(): Promise<any> {
  const pipeline = await MemFlow.workflow.entity();

  // Initialize pipeline state with empty arrays
  const initialState = {
    documentId: `doc-${Date.now()}`,
    status: 'started',
    startTime: new Date().toISOString(),
    imageRefs: [],
    extractedInfo: [],
    validationResults: [],
    finalResult: null,
    processingSteps: [],
    errors: [],
    pageSignals: {} // Track page processing signals
  };
  
  await pipeline.set<typeof initialState>(initialState);

  try {
    // Step 1: Get list of image file references
    await pipeline.merge({status: 'loading-images'});
    await pipeline.append('processingSteps', 'image-load-started');

    const imageRefs = await activities.loadImagePages();
    if (!imageRefs || imageRefs.length === 0) {
      throw new Error('No image references found');
    }

    await pipeline.merge({imageRefs});
    await pipeline.append('processingSteps', 'image-load-completed');

    // Initialize page signals tracking
    const pageSignals: Record<string, boolean> = {};
    imageRefs.forEach((_, index) => {
      pageSignals[`page-${index + 1}-complete`] = false;
    });
    await pipeline.merge({ pageSignals });

    // Launch processing hooks for each page in parallel
    for (const [index, imageRef] of imageRefs.entries()) {
      const pageNumber = index + 1;

      await MemFlow.workflow.execHook({
        taskQueue: 'pipeline',
        workflowName: 'pageProcessingHook',
        args: [imageRef, pageNumber, initialState.documentId],
        signalId: `page-${pageNumber}-complete`
      });
    };

    // Verify all pages were processed before proceeding
    const processedState = await pipeline.get() as any;
    const extractedInfo = processedState.extractedInfo || [];
    if (extractedInfo.length !== imageRefs.length) {
      throw new Error(`Not all pages were processed. Expected ${imageRefs.length}, got ${extractedInfo.length}`);
    }

    // Step 3: Launch validation hook
    await MemFlow.workflow.execHook({
      taskQueue: 'pipeline',
      workflowName: 'validationHook',
      args: [initialState.documentId],
      signalId: 'validation-complete'
    });

    // Step 4: Launch approval hook
    await MemFlow.workflow.execHook({
      taskQueue: 'pipeline',
      workflowName: 'approvalHook',
      args: [initialState.documentId],
      signalId: 'approval-complete',
    });

    // Step 5: Launch notification hook
    await MemFlow.workflow.execHook({
      taskQueue: 'pipeline',
      workflowName: 'notificationHook',
      args: [initialState.documentId],
      signalId: 'processing-complete',
    });

    // Retrieve final state    
    await pipeline.merge({status: 'completed', completedAt: new Date().toISOString()});
    await pipeline.append('processingSteps', 'pipeline-completed');

    return await pipeline.get();

  } catch (error) {
    if (MemFlow.workflow.didInterrupt(error)) {
      throw error;
    }

    await pipeline.merge({status: 'failed', error: error.message, failedAt: new Date().toISOString()});
    await pipeline.append('processingSteps', 'pipeline-failed');
    throw error;
  }
}

/**
 * Page Processing Hook
 * Processes individual PNG image pages and extracts member information
 */
export async function pageProcessingHook(imageRef: string, pageNumber: number, documentId: string): Promise<void> {
  const pipeline = await MemFlow.workflow.entity();
  const state = await pipeline.get() as any;

  try {
    // Ensure processingSteps exists
    const currentSteps = Array.isArray(state.processingSteps) ? state.processingSteps : [];
    
    // Extract member information from the page using the file reference
    const memberInfo = await activities.extractMemberInfoFromPage(imageRef, pageNumber);
    
    if (memberInfo) {
      // Update extracted information in shared state
      const updatedInfo = {
        pageNumber,
        memberInfo,
        imageRef, // Store the reference for future use if needed
        extractedAt: new Date().toISOString()
      };
      
      await pipeline.append('extractedInfo', updatedInfo);
      await pipeline.append('processingSteps', `page-${pageNumber}-processed`);
    }

    // Signal that this page processing is complete
    await MemFlow.workflow.signal(`page-${pageNumber}-complete`, {
      pageNumber,
      memberId: memberInfo?.memberId,
      imageRef, // Include reference in signal for tracking
      completed: true
    });

  } catch (error) {
    if (MemFlow.workflow.didInterrupt(error)) {
      throw error;
    }

    const errors = {
      page: pageNumber,
      imageRef, // Include reference in error for debugging
      error: (error as any).message,
      timestamp: new Date().toISOString()
    };
    
    await pipeline.append('errors', errors);
    
    // Signal completion even on error so workflow doesn't hang
    await MemFlow.workflow.signal(`page-${pageNumber}-complete`, {
      pageNumber,
      imageRef,
      error: (error as any).message,
      completed: false
    });
    
    throw error;
  }
}

/**
 * Validation Hook
 * Validates extracted member information against database
 */
export async function validationHook(documentId: string): Promise<void> {
  const pipeline = await MemFlow.workflow.entity();
  const state = await pipeline.get() as any;

  try {

    // Combine information from all pages for each member ID
    const memberDataMap = new Map<string, any>();
    let currentMemberId: string | null = null;
    
    // Process pages in order to associate partial info with the correct member
    const sortedExtracted = (state.extractedInfo || []).sort((a: any, b: any) => a.pageNumber - b.pageNumber);
    
    for (const extracted of sortedExtracted) {
      let memberId = extracted.memberInfo.memberId;
      
      // If this page has no member ID, associate it with the most recent member ID
      if (!memberId && extracted.memberInfo.isPartialInfo && currentMemberId) {
        memberId = currentMemberId;
      } else if (memberId) {
        // Update the current member ID for subsequent pages
        currentMemberId = memberId;
      }
      
      if (memberId) {
        const existing = memberDataMap.get(memberId) || { memberId, pages: [] };
        
        // Merge the member information from this page (excluding the isPartialInfo flag)
        const { isPartialInfo, ...cleanMemberInfo } = extracted.memberInfo;
        Object.assign(existing, cleanMemberInfo);
        existing.pages.push(extracted.pageNumber);
        
        memberDataMap.set(memberId, existing);
      }
    }
    
    const validationResults: any[] = [];
    
    // Validate the combined member information
    for (const [memberId, combinedInfo] of memberDataMap) {
      
      const isValid = await activities.validateMemberInfo(combinedInfo);
      validationResults.push({
        memberId,
        pages: combinedInfo.pages,
        isValid,
        validatedAt: new Date().toISOString(),
        combinedInfo
      });
    }

    await pipeline.merge({validationResults});
    await pipeline.append('processingSteps', 'validation-completed');

    // Signal that validation is complete
    await MemFlow.workflow.signal('validation-complete', { validationResults });

  } catch (error) {
    if (MemFlow.workflow.didInterrupt(error)) {
      throw error;
    }

    const errors = {
      stage: 'validation',
      error: (error as any).message,
      timestamp: new Date().toISOString()
    };
    
    await pipeline.append('errors', errors);
    throw error;
  }
}

/**
 * Approval Hook
 * Processes document approval based on validation results
 */
export async function approvalHook(documentId: string): Promise<void> {
  const pipeline = await MemFlow.workflow.entity();
  const state = await pipeline.get() as any;

  try {
    // Process approval for each validated member
    const approvalResults: any[] = [];
    
    for (const validation of state.validationResults || []) {
      const memberInfo = validation.combinedInfo;
      
      if (memberInfo) {
        const approvalResult = await activities.processDocumentApproval(memberInfo, validation.isValid);
        approvalResults.push({
          memberId: validation.memberId,
          pages: validation.pages,
          approvalResult,
          processedAt: new Date().toISOString()
        });
      }
    }

    await pipeline.merge({approvalResults});
    await pipeline.append('processingSteps', 'approval-completed');

    // Signal that approval is complete
    await MemFlow.workflow.signal('approval-complete', { approvalResults });

  } catch (error) {
    if (MemFlow.workflow.didInterrupt(error)) {
      throw error;
    }

    const errors = {
      stage: 'approval',
      error: (error as any).message,
      timestamp: new Date().toISOString()
    };
    
    await pipeline.append('errors', errors);
    throw error;
  }
}

/**
 * Notification Hook
 * Sends notifications based on processing results
 */
export async function notificationHook(documentId: string): Promise<void> {
  const pipeline = await MemFlow.workflow.entity();
  const state = await pipeline.get() as any;

  try {
    // Send notifications for each approval result
    const notificationResults: any[] = [];
    
    for (const approval of state.approvalResults || []) {
      await activities.sendNotification(approval.approvalResult);
      notificationResults.push({
        memberId: approval.memberId,
        status: approval.approvalResult.status,
        notifiedAt: new Date().toISOString()
      });
    }

    const finalResult = {
      documentId,
      totalPages: state.imageRefs?.length || 0,
      extractedMembers: state.extractedInfo?.length || 0,
      validatedMembers: state.validationResults?.filter((v: any) => v.isValid).length || 0,
      approvedMembers: state.approvalResults?.filter((a: any) => a.approvalResult.status === 'approved').length || 0,
      notifications: notificationResults.length,
      processingTime: new Date().getTime() - new Date(state.startTime).getTime(),
      status: 'completed'
    };

    await pipeline.merge({finalResult, notificationResults});
    await pipeline.append('processingSteps', 'notifications-sent');
    await pipeline.append('processingSteps', 'notifications-completed');

    // Signal that processing is complete
    await MemFlow.workflow.signal('processing-complete', { finalResult });

  } catch (error) {
    if (MemFlow.workflow.didInterrupt(error)) {
      throw error;
    }
    const errors = {
      stage: 'notification',
      error: (error as any).message,
      timestamp: new Date().toISOString()
    };
    
    await pipeline.append('errors', errors);
    throw error;
  }
}

/**
 * Basic Test Workflow
 * Simple test to verify pipeline functionality without OpenAI
 */
export async function basicPipelineTest(testName: string): Promise<any> {
  const pipeline = await MemFlow.workflow.entity();
  
  const testState = {
    testName,
    status: 'started',
    operations: ['test-started'],
    startTime: new Date().toISOString(),
    completedAt: null
  };
  
  await pipeline.set(testState);

  // Simulate pipeline operations
  await new Promise(resolve => setTimeout(resolve, 1000));
  
  const finalState = {
    ...testState,
    status: 'completed',
    operations: ['test-started', 'test-completed'],
    completedAt: new Date().toISOString()
  };
  
  await pipeline.merge(finalState);
  
  return await pipeline.get();
}
