import { Durable } from '../../../../services/durable';

export default async function greet(
  name: string,
): Promise<{ complex: string }> {
  return { complex: `Hello, ${name}!` };
}

export async function processOrder(orderId: string): Promise<string> {
  return `order-${orderId}-processed`;
}

export async function sendNotification(
  target: string,
  message: string,
): Promise<string> {
  return `notified:${target}:${message}`;
}

export async function auditLog(
  workflowId: string,
  action: string,
): Promise<string> {
  return `audit:${workflowId}:${action}`;
}

export async function metricsCollect(
  workflowId: string,
  metric: string,
  value: number,
): Promise<void> {
  // Simulate metrics collection
}

export async function metadataAwareActivity(
  data: string,
): Promise<Record<string, any>> {
  const ctx = Durable.activity.getContext();
  return {
    data,
    activityName: ctx.activityName,
    headers: ctx.headers,
  };
}
