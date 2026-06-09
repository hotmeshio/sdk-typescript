import { StreamDataType } from '../../../types/stream';
import { DuressLevel } from '../../../types/quorum';
import {
  HMSH_DURESS_ALPHA,
  HMSH_DURESS_HEALTHY_CEILING_MS,
  HMSH_DURESS_MILD_CEILING_MS,
  HMSH_DURESS_MODERATE_CEILING_MS,
  HMSH_DURESS_BROADCAST_INTERVAL_MS,
  HMSH_DURESS_HYSTERESIS_COUNT,
} from '../config';

export interface DuressSnapshot {
  level: DuressLevel;
  score_ms: number;
  throttle_ms: number;
  per_type: Record<string, number>;
}

// Throttle band boundaries (ms)
const MILD_THROTTLE_MIN = 100;
const MILD_THROTTLE_MAX = 500;
const MODERATE_THROTTLE_MIN = 500;
const MODERATE_THROTTLE_MAX = 2000;
const SEVERE_THROTTLE_MIN = 2000;
const SEVERE_THROTTLE_MAX = 5000;

/**
 * Adaptive engine duress detection via processing latency.
 *
 * ## Why this exists
 *
 * Prior fixes responded to queue *depth* (a symptom) — doubling reservation
 * timeouts and halving batch sizes when the stream backed up. A deep queue
 * doesn't necessarily mean duress (it could be a burst of external triggers),
 * and a shallow queue doesn't necessarily mean health. This module responds
 * to the *cause*: actual processing latency per message type.
 *
 * ## How it works
 *
 * Each engine router tracks an exponential moving average (EMA) of how long
 * each canonical message type (transition, timehook, webhook, worker response,
 * etc.) takes to process. When healthy, these are sub-50ms. When the max EMA
 * crosses configurable thresholds (200ms → mild, 1s → moderate, 5s → severe),
 * the manager computes a proportional throttle delay that the ThrottleManager
 * applies as a floor on engine consumption rate.
 *
 * ## Hysteresis (asymmetric by design)
 *
 * Escalation is immediate — if the engine suddenly enters duress, the throttle
 * kicks in on the next evaluation. De-escalation requires `HYSTERESIS_COUNT`
 * (default 3) consecutive improving evaluations before dropping a level. This
 * prevents oscillation: throttle → drain → un-throttle → refill → throttle.
 * The EMA already smooths individual outliers; hysteresis gates the recovery
 * path specifically.
 *
 * ## Quorum coordination
 *
 * When a router detects a level change (or remains in duress), it broadcasts
 * a `'duress'` message via the quorum. Peers adopt the signal only if it's
 * worse than their local state, so the mesh converges on the worst-case
 * throttle without coordination.
 *
 * ## What this does NOT do
 *
 * External messages (triggers, signalIn/webhooks from the outside world) are
 * never throttled. They always enter `engine_streams`. Only the engine
 * routers' pull rate slows down, giving the system breathing room.
 */
export class DuressManager {
  // Per-message-type exponential moving averages
  private emas: Map<string, number> = new Map();
  private sampleCounts: Map<string, number> = new Map();

  // Hysteresis state
  private currentLevel: DuressLevel = 'healthy';
  private belowThresholdCount = 0;

  // Computed duress throttle floor
  private duressThrottle = 0;

  // Broadcast rate limiting
  private lastBroadcastAt = 0;
  private lastBroadcastLevel: DuressLevel = 'healthy';

  /**
   * Record a processing duration for a message type.
   * Updates the exponential moving average for that type.
   */
  recordLatency(type: StreamDataType, durationMs: number): void {
    const key = type as string;
    const count = this.sampleCounts.get(key) || 0;
    if (count === 0) {
      // First sample: seed the EMA directly
      this.emas.set(key, durationMs);
    } else {
      const prev = this.emas.get(key)!;
      this.emas.set(
        key,
        HMSH_DURESS_ALPHA * durationMs + (1 - HMSH_DURESS_ALPHA) * prev,
      );
    }
    this.sampleCounts.set(key, count + 1);
  }

  /**
   * Evaluate duress state from current EMAs.
   * Returns a snapshot with level, score, recommended throttle,
   * and per-type latencies.
   */
  evaluate(): DuressSnapshot {
    // Aggregate: max EMA across all tracked types
    let maxEma = 0;
    const perType: Record<string, number> = {};
    for (const [type, ema] of this.emas) {
      perType[type] = Math.round(ema);
      if (ema > maxEma) maxEma = ema;
    }

    const rawLevel = this.scoreToLevel(maxEma);

    // Hysteresis: only drop level after sustained improvement
    if (this.levelOrdinal(rawLevel) < this.levelOrdinal(this.currentLevel)) {
      this.belowThresholdCount++;
      if (this.belowThresholdCount >= HMSH_DURESS_HYSTERESIS_COUNT) {
        this.currentLevel = rawLevel;
        this.belowThresholdCount = 0;
      }
      // Keep current (higher) level until hysteresis clears
    } else {
      // Same or worse: reset hysteresis counter, adopt immediately
      this.belowThresholdCount = 0;
      this.currentLevel = rawLevel;
    }

    this.duressThrottle =
      this.currentLevel === 'healthy'
        ? 0
        : this.scoreToThrottle(maxEma, this.currentLevel);

    return {
      level: this.currentLevel,
      score_ms: Math.round(maxEma),
      throttle_ms: this.duressThrottle,
      per_type: perType,
    };
  }

  getDuressThrottle(): number {
    return this.duressThrottle;
  }

  getCurrentLevel(): DuressLevel {
    return this.currentLevel;
  }

  /**
   * Apply a duress snapshot received from another engine via quorum.
   * Adopts the remote signal only if it indicates worse duress than local.
   */
  applyRemoteDuress(throttleMs: number, level: DuressLevel): void {
    if (this.levelOrdinal(level) > this.levelOrdinal(this.currentLevel)) {
      this.currentLevel = level;
      this.duressThrottle = throttleMs;
      this.belowThresholdCount = 0;
    }
  }

  /**
   * Whether a quorum broadcast is warranted.
   * Rate-limited and only fires when level changes or duress is active.
   */
  shouldBroadcast(): boolean {
    const now = Date.now();
    if (now - this.lastBroadcastAt < HMSH_DURESS_BROADCAST_INTERVAL_MS) {
      return false;
    }
    return (
      this.currentLevel !== this.lastBroadcastLevel ||
      this.currentLevel !== 'healthy'
    );
  }

  markBroadcast(): void {
    this.lastBroadcastAt = Date.now();
    this.lastBroadcastLevel = this.currentLevel;
  }

  /**
   * Returns a snapshot for inclusion in quorum rollcall profiles.
   */
  getSnapshot(): DuressSnapshot {
    let maxEma = 0;
    const perType: Record<string, number> = {};
    for (const [type, ema] of this.emas) {
      perType[type] = Math.round(ema);
      if (ema > maxEma) maxEma = ema;
    }
    return {
      level: this.currentLevel,
      score_ms: Math.round(maxEma),
      throttle_ms: this.duressThrottle,
      per_type: perType,
    };
  }

  // --- Private helpers ---

  private scoreToLevel(ms: number): DuressLevel {
    if (ms < HMSH_DURESS_HEALTHY_CEILING_MS) return 'healthy';
    if (ms < HMSH_DURESS_MILD_CEILING_MS) return 'mild';
    if (ms < HMSH_DURESS_MODERATE_CEILING_MS) return 'moderate';
    return 'severe';
  }

  private scoreToThrottle(ms: number, level: DuressLevel): number {
    // Linear interpolation within the band for the given level
    switch (level) {
      case 'healthy':
        return 0;
      case 'mild':
        return this.lerp(
          ms,
          HMSH_DURESS_HEALTHY_CEILING_MS,
          HMSH_DURESS_MILD_CEILING_MS,
          MILD_THROTTLE_MIN,
          MILD_THROTTLE_MAX,
        );
      case 'moderate':
        return this.lerp(
          ms,
          HMSH_DURESS_MILD_CEILING_MS,
          HMSH_DURESS_MODERATE_CEILING_MS,
          MODERATE_THROTTLE_MIN,
          MODERATE_THROTTLE_MAX,
        );
      case 'severe':
        // Clamp to severe band max; beyond the ceiling is still severe
        return this.lerp(
          ms,
          HMSH_DURESS_MODERATE_CEILING_MS,
          HMSH_DURESS_MODERATE_CEILING_MS * 2,
          SEVERE_THROTTLE_MIN,
          SEVERE_THROTTLE_MAX,
        );
    }
  }

  private lerp(
    value: number,
    inMin: number,
    inMax: number,
    outMin: number,
    outMax: number,
  ): number {
    const t = Math.min(Math.max((value - inMin) / (inMax - inMin), 0), 1);
    return Math.round(outMin + t * (outMax - outMin));
  }

  private levelOrdinal(level: DuressLevel): number {
    switch (level) {
      case 'healthy':
        return 0;
      case 'mild':
        return 1;
      case 'moderate':
        return 2;
      case 'severe':
        return 3;
    }
  }
}
