/**
 * Time notification system — no longer needed.
 *
 * Sleep timers are now engine_streams messages with future visible_at.
 * The existing engine_streams NOTIFY triggers and the scout's
 * notify_visible_messages() handle waking consumers when messages
 * become visible. No separate timer infrastructure required.
 */
export function getTimeNotifySql(_schema: string): string {
  return '';
}
