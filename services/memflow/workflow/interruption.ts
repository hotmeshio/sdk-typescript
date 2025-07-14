import {
  MemFlowChildError,
  MemFlowFatalError,
  MemFlowMaxedError,
  MemFlowProxyError,
  MemFlowRetryError,
  MemFlowSleepError,
  MemFlowTimeoutError,
  MemFlowWaitForError,
  MemFlowWaitForAllError,
} from '../../../modules/errors';

/**
 * Checks if an error is a HotMesh reserved error type that indicates
 * a HotMesh interruption rather than a true error condition.
 * 
 * When this returns true, you can safely return rethrow the error.
 * The workflow engine will handle the interruption automatically.
 * 
 * @example
 * ```typescript
 * import { MemFlow } from '@hotmeshio/hotmesh';
 * 
 * try {
 *   await someWorkflowOperation();
 * } catch (error) {
 *   // Check if this is a HotMesh interruption
 *   if (MemFlow.workflow.didInterrupt(error)) {
 *     // Rethrow the error
 *     throw error;
 *   }
 *   // Handle actual error
 *   console.error('Workflow failed:', error);
 * }
 * ```
 * 
 * @param error - The error to check
 * @returns true if the error is a HotMesh interruption
 */
export function didInterrupt(error: Error): boolean {
  return (
    error instanceof MemFlowChildError ||
    error instanceof MemFlowFatalError ||
    error instanceof MemFlowMaxedError ||
    error instanceof MemFlowProxyError ||
    error instanceof MemFlowRetryError ||
    error instanceof MemFlowSleepError ||
    error instanceof MemFlowTimeoutError ||
    error instanceof MemFlowWaitForError ||
    error instanceof MemFlowWaitForAllError
  );
} 