import { MAX_DELAY } from '../config';

export class ThrottleManager {
  private throttle: number = 0;
  private isSleeping: boolean = false;
  private sleepPromiseResolve: (() => void) | null = null;
  private innerPromiseResolve: (() => void) | null = null;
  private sleepTimeout: NodeJS.Timeout | null = null;

  constructor(initialThrottle: number = 0) {
    this.throttle = initialThrottle;
  }

  getThrottle(): number {
    return this.throttle;
  }

  setThrottle(delayInMillis: number): void {
    const wasDecreased = delayInMillis < this.throttle;
    this.throttle = delayInMillis;

    // If the throttle was decreased, and we're in the middle of a sleep cycle, adjust immediately
    if (wasDecreased) {
      if (this.sleepTimeout) {
        clearTimeout(this.sleepTimeout);
      }
      if (this.innerPromiseResolve) {
        this.innerPromiseResolve();
      }
    }
  }

  isPaused(): boolean {
    return this.throttle === MAX_DELAY;
  }

  /**
   * An adjustable throttle that will interrupt a sleeping
   * router if the throttle is reduced and the sleep time
   * has elapsed. If the throttle is increased, or if
   * the sleep time has not elapsed, the router will continue
   * to sleep until the new termination point. This
   * allows for dynamic, elastic throttling with smooth
   * acceleration and deceleration.
   */
  async customSleep(): Promise<void> {
    if (this.throttle === 0) return;
    if (this.isSleeping) return;
    this.isSleeping = true;
    const startTime = Date.now(); //anchor the origin

    await new Promise<void>(async (outerResolve) => {
      this.sleepPromiseResolve = outerResolve;
      let elapsedTime = Date.now() - startTime;
      while (elapsedTime < this.throttle) {
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