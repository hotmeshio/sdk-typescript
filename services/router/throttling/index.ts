/**
 * Elastic throttle with two independent inputs:
 *
 * 1. **User throttle** — set explicitly via quorum `throttle` command.
 *    Absolute value: 0 = resume, >0 = delay per message, -1 = pause.
 *
 * 2. **Duress floor** — set automatically by the DuressManager based on
 *    processing latency. The effective throttle is `max(user, duress)`,
 *    so duress never reduces below what the user set, and pause always
 *    takes precedence. When duress clears (floor returns to 0), the
 *    user's original throttle remains in effect.
 *
 * `customSleep()` uses the effective throttle, supports dynamic
 * interruption (if the throttle decreases mid-sleep, the router wakes
 * early), and handles pause via a bare promise with no timer.
 */
export class ThrottleManager {
  private throttle = 0;
  private duressFloor = 0;
  private isSleeping = false;
  private sleepPromiseResolve: (() => void) | null = null;
  private innerPromiseResolve: (() => void) | null = null;
  private sleepTimeout: NodeJS.Timeout | null = null;

  constructor(initialThrottle = 0) {
    this.throttle = initialThrottle;
  }

  getThrottle(): number {
    return this.throttle;
  }

  /**
   * Set the duress-computed throttle floor. The effective throttle
   * is max(userThrottle, duressFloor). Pause (throttle < 0) overrides.
   */
  setDuressFloor(delayMs: number): void {
    this.duressFloor = Math.max(0, delayMs);
  }

  getDuressFloor(): number {
    return this.duressFloor;
  }

  /**
   * Returns the effective throttle: max of user-set throttle and
   * duress floor. Pause (negative) always takes precedence.
   */
  getEffectiveThrottle(): number {
    if (this.throttle < 0) return this.throttle; // pause overrides
    return Math.max(this.throttle, this.duressFloor);
  }

  setThrottle(delayInMillis: number): void {
    const wasPaused = this.throttle < 0;
    const wasDecreased = delayInMillis < this.throttle;
    this.throttle = delayInMillis;

    // Interrupt sleep when: resuming from pause, or delay was decreased
    if (wasPaused || wasDecreased) {
      if (this.sleepTimeout) {
        clearTimeout(this.sleepTimeout);
        this.sleepTimeout = null;
      }
      if (this.innerPromiseResolve) {
        this.innerPromiseResolve();
      }
    }
  }

  isPaused(): boolean {
    return this.throttle < 0;
  }

  /**
   * An adjustable throttle that will interrupt a sleeping
   * router if the throttle is reduced and the sleep time
   * has elapsed. If the throttle is increased, or if
   * the sleep time has not elapsed, the router will continue
   * to sleep until the new termination point. This
   * allows for dynamic, elastic throttling with smooth
   * acceleration and deceleration.
   *
   * When paused (throttle < 0), waits indefinitely via a bare
   * promise — no setTimeout timer. Resumes instantly when
   * setThrottle() is called with a non-negative value.
   */
  async customSleep(): Promise<void> {
    const effective = this.getEffectiveThrottle();
    if (effective === 0) return;
    if (this.isSleeping) return;
    this.isSleeping = true;

    if (effective < 0) {
      // Paused: wait indefinitely until setThrottle interrupts
      await new Promise<void>((resolve) => {
        this.innerPromiseResolve = resolve;
      });
      this.resetThrottleState();
      return;
    }

    const startTime = Date.now();

    await new Promise<void>(async (outerResolve) => {
      this.sleepPromiseResolve = outerResolve;
      let elapsedTime = Date.now() - startTime;
      let target = this.getEffectiveThrottle();
      while (elapsedTime < target && target > 0) {
        await new Promise<void>((innerResolve) => {
          this.innerPromiseResolve = innerResolve;
          this.sleepTimeout = setTimeout(
            innerResolve,
            target - elapsedTime,
          );
        });
        elapsedTime = Date.now() - startTime;
        target = this.getEffectiveThrottle();
      }
      this.resetThrottleState();
      outerResolve();
    });
  }

  cancelThrottle(): void {
    if (this.sleepTimeout) {
      clearTimeout(this.sleepTimeout);
    }
    this.resetThrottleState();
  }

  private resetThrottleState(): void {
    this.sleepPromiseResolve = null;
    this.innerPromiseResolve = null;
    this.isSleeping = false;
    this.sleepTimeout = null;
  }
}
