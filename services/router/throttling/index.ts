export class ThrottleManager {
  private throttle = 0;
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
    if (this.throttle === 0) return;
    if (this.isSleeping) return;
    this.isSleeping = true;

    if (this.throttle < 0) {
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
      while (elapsedTime < this.throttle && this.throttle > 0) {
        await new Promise<void>((innerResolve) => {
          this.innerPromiseResolve = innerResolve;
          this.sleepTimeout = setTimeout(
            innerResolve,
            this.throttle - elapsedTime,
          );
        });
        elapsedTime = Date.now() - startTime;
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
