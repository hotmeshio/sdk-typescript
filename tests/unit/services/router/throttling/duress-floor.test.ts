import { describe, it, expect, beforeEach } from 'vitest';
import { ThrottleManager } from '../../../../../services/router/throttling';

describe('ThrottleManager — duress floor', () => {
  let manager: ThrottleManager;

  beforeEach(() => {
    manager = new ThrottleManager(0);
  });

  describe('getEffectiveThrottle', () => {
    it('returns 0 when no user throttle and no duress', () => {
      expect(manager.getEffectiveThrottle()).toBe(0);
    });

    it('returns user throttle when higher than duress floor', () => {
      manager.setThrottle(1000);
      manager.setDuressFloor(500);
      expect(manager.getEffectiveThrottle()).toBe(1000);
    });

    it('returns duress floor when higher than user throttle', () => {
      manager.setThrottle(200);
      manager.setDuressFloor(2000);
      expect(manager.getEffectiveThrottle()).toBe(2000);
    });

    it('pause overrides duress floor', () => {
      manager.setDuressFloor(5000);
      manager.setThrottle(-1);
      expect(manager.getEffectiveThrottle()).toBe(-1);
    });

    it('duress floor of 0 has no effect', () => {
      manager.setThrottle(500);
      manager.setDuressFloor(0);
      expect(manager.getEffectiveThrottle()).toBe(500);
    });

    it('negative duress floor is clamped to 0', () => {
      manager.setDuressFloor(-100);
      expect(manager.getDuressFloor()).toBe(0);
      expect(manager.getEffectiveThrottle()).toBe(0);
    });
  });

  describe('customSleep with duress floor', () => {
    it('returns immediately when effective throttle is 0', async () => {
      manager.setDuressFloor(0);
      manager.setThrottle(0);
      const start = Date.now();
      await manager.customSleep();
      expect(Date.now() - start).toBeLessThan(50);
    });

    it('sleeps for duress floor duration when user throttle is 0', async () => {
      manager.setDuressFloor(100);
      const start = Date.now();
      await manager.customSleep();
      const elapsed = Date.now() - start;
      // Should have slept approximately 100ms
      expect(elapsed).toBeGreaterThanOrEqual(80);
      expect(elapsed).toBeLessThan(250);
    });
  });
});
