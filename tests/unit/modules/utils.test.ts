import { describe, it, expect } from 'vitest';
import * as utils from '../../../modules/utils';
import { StreamStatus } from '../../../types/stream';

describe('utils module', () => {
  describe('utils.matchesStatusCode function', () => {
    it('should match 404 with "4**" pattern', () => {
      expect(utils.matchesStatusCode(404, '4**')).toBe(true);
    });

    it('should not match 304 with "4**" pattern', () => {
      expect(utils.matchesStatusCode(304, '4**')).toBe(false);
    });

    it('should match 404 with /^4\\d\\d$/ pattern', () => {
      expect(utils.matchesStatusCode(404, /^4\d\d$/)).toBe(true);
    });

    it('should match 403 and 404 with /^40[34]$/ pattern', () => {
      expect(utils.matchesStatusCode(403, /^40[34]$/)).toBe(true);
      expect(utils.matchesStatusCode(404, /^40[34]$/)).toBe(true);
    });

    it('should not match 405 with /^40[34]$/ pattern', () => {
      expect(utils.matchesStatusCode(405, /^40[34]$/)).toBe(false);
    });

    it('should not match 500 with "4**" pattern', () => {
      expect(utils.matchesStatusCode(500, '4**')).toBe(false);
    });

    it('should match 500 with "5**" pattern', () => {
      expect(utils.matchesStatusCode(500, '5**')).toBe(true);
    });
  });

  describe('utils.matchesStatus function', () => {
    it('should match StreamStatus.SUCCESS with StreamStatus.SUCCESS', () => {
      expect(
        utils.matchesStatus(StreamStatus.SUCCESS, StreamStatus.SUCCESS),
      ).toBe(true);
    });

    it('should not match StreamStatus.SUCCESS with StreamStatus.ERROR', () => {
      expect(
        utils.matchesStatus(StreamStatus.SUCCESS, StreamStatus.ERROR),
      ).toBe(false);
    });

    it('should match StreamStatus.ERROR with StreamStatus.ERROR', () => {
      expect(utils.matchesStatus(StreamStatus.ERROR, StreamStatus.ERROR)).toBe(
        true,
      );
    });

    it('should not match StreamStatus.ERROR with StreamStatus.PENDING', () => {
      expect(
        utils.matchesStatus(StreamStatus.ERROR, StreamStatus.PENDING),
      ).toBe(false);
    });

    it('should match StreamStatus.PENDING with StreamStatus.PENDING', () => {
      expect(
        utils.matchesStatus(StreamStatus.PENDING, StreamStatus.PENDING),
      ).toBe(true);
    });
  });

  describe('getSymKey function', () => {
    it('should return "aaa" for input 0', () => {
      const sequence = utils.getSymKey(0);
      expect(sequence).toBe('aaa');
    });

    it('should return "aba" for input 1', () => {
      //verify sequence increments position 2
      const sequence = utils.getSymKey(1);
      expect(sequence).toBe('aba');
    });

    it('should return "aAf" for input 286', () => {
      //verify sequence for 286 (26 metadata slots, 260 data slots)
      const sequence = utils.getSymKey(286);
      expect(sequence).toBe('aAf');
    });

    it('should return "ZZZ" for input 140607', () => {
      //length of the alphabet squared, minus 1 for zero indexing
      const maxAllowed = Math.pow(52, 3) - 1;
      const sequence = utils.getSymKey(maxAllowed);
      expect(sequence).toBe('ZZZ');
    });

    it('should return the max allowed value without throwing an error', () => {
      const maxAllowed = Math.pow(52, 3) - 1;
      expect(() => utils.getSymKey(maxAllowed)).not.toThrow();
    });

    it('should throw an error for input greater than the max allowed value', () => {
      const tooLarge = Math.pow(52, 3); // length of the alphabet squared
      expect(() => utils.getSymKey(tooLarge)).toThrow('Number out of range');
    });
  });

  describe('getSymVal function', () => {
    it('should return "aa" for input 0', () => {
      const sequence = utils.getSymVal(0);
      expect(sequence).toBe('aa');
    });

    it('should return "ab" for input 1', () => {
      //verify sequence increments position 2
      const sequence = utils.getSymVal(1);
      expect(sequence).toBe('ab');
    });

    it('should return "aA" for input 26', () => {
      //verify sequence for 26 (26 metadata slots, 26 data slots)
      const sequence = utils.getSymVal(26);
      expect(sequence).toBe('aA');
    });

    it('should return "ZZ" for input 2703', () => {
      //length of the alphabet squared, minus 1 for zero indexing
      const maxAllowed = Math.pow(52, 2) - 1;
      const sequence = utils.getSymVal(maxAllowed);
      expect(sequence).toBe('ZZ');
    });

    it('should return the max allowed value without throwing an error', () => {
      const maxAllowed = Math.pow(52, 2) - 1;
      expect(() => utils.getSymVal(maxAllowed)).not.toThrow();
    });

    it('should throw an error for input greater than the max allowed value', () => {
      const tooLarge = Math.pow(52, 2); // length of the alphabet squared
      expect(() => utils.getSymVal(tooLarge)).toThrow('Number out of range');
    });
  });
});
