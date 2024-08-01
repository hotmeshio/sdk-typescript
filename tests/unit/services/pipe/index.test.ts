import { Pipe } from '../../../../services/pipe';
import { Pipe as PipeType } from '../../../../types/pipe';

describe('Pipe', () => {
  let pipe: Pipe | null;
  let rules: PipeType;
  let jobData: { [key: string]: any };

  beforeEach(() => {
    rules = [];
    jobData = {};
  });

  afterEach(() => {
    pipe = null;
  });

  describe('date', () => {
    it('now, toLocaleString', () => {
      jobData = {};

      // Test now
      rules = [['{@date.now}']];
      const before = Date.now();
      pipe = new Pipe(rules, jobData);
      const now = pipe.process();
      const after = Date.now();
      expect(now).toBeGreaterThanOrEqual(before);
      expect(now).toBeLessThanOrEqual(after);

      rules = [['{@date.now}', 'en-US'], ['{@date.toLocaleString}']];

      pipe = new Pipe(rules, jobData);
      const localeString = pipe.process();
      //"8/6/2023, 10:52:39 PM"
      expect(!isNaN(localeString.split('/')[0])).toEqual(true);
      expect(!isNaN(localeString.split('/')[1])).toEqual(true);
      expect(!isNaN(localeString.split('/')[2])).toEqual(false);
    });
  });

  describe('array', () => {
    it('should join an array with a space delimiter', () => {
      jobData = {
        names: ['Luke', 'Birdeau'],
      };

      rules = [['{names}', ' '], ['{@array.join}']];

      pipe = new Pipe(rules, jobData);
      const result = pipe.process();

      expect(result).toEqual('Luke Birdeau');
    });
  });

  describe('cron', () => {
    it('should calculate the next delay for a cron job', () => {
      jobData = {
        cronExpression: '0 0 * * *',
      };

      rules = [['{cronExpression}'], ['{@cron.nextDelay}']];

      pipe = new Pipe(rules, jobData);
      let result = pipe.process();

      expect(result).toBeGreaterThan(0); //integer (in seconds)
      jobData = {
        cronExpression: '1 day',
      };

      rules = [['{cronExpression}'], ['{@cron.nextDelay}']];

      pipe = new Pipe(rules, jobData);
      result = pipe.process();

      expect(result).toBe(-1); //invalid cron expression
    });
  });

  describe('number', () => {
    it('isEven, isOdd', () => {
      jobData = {
        evenNumber: 42,
        oddNumber: 55,
      };

      // Test isEven function
      rules = [['{evenNumber}'], ['{@number.isEven}']];

      pipe = new Pipe(rules, jobData);
      const isEvenResult = pipe.process();

      expect(isEvenResult).toEqual(true);

      // Test isOdd function
      rules = [['{oddNumber}'], ['{@number.isOdd}']];

      pipe = new Pipe(rules, jobData);
      const isOddResult = pipe.process();

      expect(isOddResult).toEqual(true);
    });
  });

  describe('string', () => {
    it('concat, split, charAt, includes, indexOf', () => {
      jobData = {
        fullName: 'Luke Birdeau',
        searchString: 'Birdeau',
        firstName: 'Luke',
        lastName: 'Birdeau',
      };

      //Concat two strings
      rules = [['{firstName}', '{lastName}'], ['{@string.concat}']];

      pipe = new Pipe(rules, jobData);
      const concatResult = pipe.process();

      expect(concatResult).toEqual('LukeBirdeau');

      // Split the full name into words
      rules = [['{fullName}', ' '], ['{@string.split}']];
      pipe = new Pipe(rules, jobData);
      const splitResult = pipe.process();
      expect(splitResult).toEqual(['Luke', 'Birdeau']);

      // Get the first character of the first name
      rules = [['{fullName}', 0], ['{@string.charAt}']];
      pipe = new Pipe(rules, jobData);
      const charAtResult = pipe.process();
      expect(charAtResult).toEqual('L');

      // Check if the full name includes the search string
      rules = [['{fullName}', '{searchString}'], ['{@string.includes}']];
      pipe = new Pipe(rules, jobData);
      const includesResult = pipe.process();
      expect(includesResult).toEqual(true);

      // Get the index of the search string in the full name
      rules = [['{fullName}', '{searchString}'], ['{@string.indexOf}']];
      pipe = new Pipe(rules, jobData);
      const indexOfResult = pipe.process();
      expect(indexOfResult).toEqual(5);
    });

    it('lastIndexOf, slice, startsWith, endsWith', () => {
      jobData = {
        fullName: 'Luke Birdeau',
        searchString: 'Birdeau',
      };

      // Get the last index of the search string in the full name
      rules = [['{fullName}', '{searchString}'], ['{@string.lastIndexOf}']];
      pipe = new Pipe(rules, jobData);
      const lastIndexOfResult = pipe.process();
      expect(lastIndexOfResult).toEqual(5);

      // Get a slice of the full name
      rules = [['{fullName}', 0, 4], ['{@string.slice}']];
      pipe = new Pipe(rules, jobData);
      const sliceResult = pipe.process();
      expect(sliceResult).toEqual('Luke');

      // Check if the full name starts with the search string
      rules = [['{fullName}', '{searchString}'], ['{@string.startsWith}']];
      pipe = new Pipe(rules, jobData);
      const startsWithResult = pipe.process();
      expect(startsWithResult).toEqual(false);

      // Check if the full name ends with the search string
      rules = [['{fullName}', '{searchString}'], ['{@string.endsWith}']];
      pipe = new Pipe(rules, jobData);
      const endsWithResult = pipe.process();
      expect(endsWithResult).toEqual(true);
    });

    it('substring, trim, trimStart, trimEnd', () => {
      jobData = {
        fullName: '  Luke Birdeau  ',
        startIndex: 2,
        endIndex: 6,
      };

      // Get a substring of the full name using startIndex and endIndex
      rules = [
        ['{fullName}', '{startIndex}', '{endIndex}'],
        ['{@string.substring}'],
      ];
      pipe = new Pipe(rules, jobData);
      const substringResult = pipe.process();
      expect(substringResult).toEqual('Luke');

      // Trim the full name
      rules = [['{fullName}'], ['{@string.trim}']];
      pipe = new Pipe(rules, jobData);
      const trimResult = pipe.process();
      expect(trimResult).toEqual('Luke Birdeau');

      // Trim the start of the full name
      rules = [['{fullName}'], ['{@string.trimStart}']];
      pipe = new Pipe(rules, jobData);
      const trimStartResult = pipe.process();
      expect(trimStartResult).toEqual('Luke Birdeau  ');

      // Trim the end of the full name
      rules = [['{fullName}'], ['{@string.trimEnd}']];
      pipe = new Pipe(rules, jobData);
      const trimEndResult = pipe.process();
      expect(trimEndResult).toEqual('  Luke Birdeau');
    });

    it('padStart, padEnd, repeat, toUpperCase', () => {
      jobData = {
        text: 'Luke',
        targetLength: 8,
        padString: '-',
        repeatCount: 3,
      };

      // Pad the start of the text
      rules = [
        ['{text}', '{targetLength}', '{padString}'],
        ['{@string.padStart}'],
      ];
      pipe = new Pipe(rules, jobData);
      const padStartResult = pipe.process();
      expect(padStartResult).toEqual('----Luke');

      // Pad the end of the text
      rules = [
        ['{text}', '{targetLength}', '{padString}'],
        ['{@string.padEnd}'],
      ];
      pipe = new Pipe(rules, jobData);
      const padEndResult = pipe.process();
      expect(padEndResult).toEqual('Luke----');

      // Repeat the text
      rules = [['{text}', '{repeatCount}'], ['{@string.repeat}']];
      pipe = new Pipe(rules, jobData);
      const repeatResult = pipe.process();
      expect(repeatResult).toEqual('LukeLukeLuke');

      // Convert the text to upper case
      rules = [['{text}'], ['{@string.toUpperCase}']];
      pipe = new Pipe(rules, jobData);
      const toUpperCaseResult = pipe.process();
      expect(toUpperCaseResult).toEqual('LUKE');
    });

    it('toLowerCase, trim, trimStart, trimEnd', () => {
      jobData = {
        text: '  Luke  ',
        upperText: 'LUKE',
      };

      // Convert the text to lower case
      rules = [['{upperText}'], ['{@string.toLowerCase}']];
      pipe = new Pipe(rules, jobData);
      const toLowerCaseResult = pipe.process();
      expect(toLowerCaseResult).toEqual('luke');

      // Trim the text
      rules = [['{text}'], ['{@string.trim}']];
      pipe = new Pipe(rules, jobData);
      const trimResult = pipe.process();
      expect(trimResult).toEqual('Luke');

      // Trim the start of the text
      rules = [['{text}'], ['{@string.trimStart}']];
      pipe = new Pipe(rules, jobData);
      const trimStartResult = pipe.process();
      expect(trimStartResult).toEqual('Luke  ');

      // Trim the end of the text
      rules = [['{text}'], ['{@string.trimEnd}']];
      pipe = new Pipe(rules, jobData);
      const trimEndResult = pipe.process();
      expect(trimEndResult).toEqual('  Luke');
    });
  });

  describe('math', () => {
    it('abs, acos, acosh, tanh, trunc, atan2, hypot', () => {
      const jobData = {
        a: {
          output: {
            data: {
              num1: -42,
              num2: 36,
              num3: 1,
              num4: 2,
              num5: 0.5,
            },
          },
        },
      };

      // Test Math.abs()
      const rulesAbs = [['{a.output.data.num1}'], ['{@math.abs}']];
      const pipeAbs = new Pipe(rulesAbs, jobData);
      expect(pipeAbs.process()).toBe(42);

      // Test Math.acos()
      const rulesAcos = [['{a.output.data.num5}'], ['{@math.acos}']];
      const pipeAcos = new Pipe(rulesAcos, jobData);
      expect(pipeAcos.process()).toBeCloseTo(Math.acos(0.5));

      // Test Math.acosh()
      const rulesAcosh = [['{a.output.data.num4}'], ['{@math.acosh}']];
      const pipeAcosh = new Pipe(rulesAcosh, jobData);
      expect(pipeAcosh.process()).toBeCloseTo(Math.acosh(2));

      // Test Math.tanh()
      const rulesTanh = [['{a.output.data.num3}'], ['{@math.tanh}']];
      const pipeTanh = new Pipe(rulesTanh, jobData);
      expect(pipeTanh.process()).toBeCloseTo(Math.tanh(1));

      // Test Math.trunc()
      const rulesTrunc = [['{a.output.data.num2}'], ['{@math.trunc}']];
      const pipeTrunc = new Pipe(rulesTrunc, jobData);
      expect(pipeTrunc.process()).toBe(36);
    });

    it('atan2', () => {
      const jobData = {
        a: {
          output: {
            data: {
              num1: 1,
              num2: 1,
            },
          },
        },
      };

      const rulesAtan2 = [
        ['{a.output.data.num1}', '{a.output.data.num2}'],
        ['{@math.atan2}'],
      ];
      const pipeAtan2 = new Pipe(rulesAtan2, jobData);
      expect(pipeAtan2.process()).toBeCloseTo(Math.atan2(1, 1));
    });

    it('hypot', () => {
      const jobData = {
        a: {
          output: {
            data: {
              num1: 3,
              num2: 4,
            },
          },
        },
      };

      const rulesHypot = [
        ['{a.output.data.num1}', '{a.output.data.num2}'],
        ['{@math.hypot}'],
      ];
      const pipeHypot = new Pipe(rulesHypot, jobData);
      expect(pipeHypot.process()).toBeCloseTo(Math.hypot(3, 4));
    });

    it('max', () => {
      const jobData = {
        a: {
          output: {
            data: {
              num1: 5,
              num2: 7,
              num3: 3,
            },
          },
        },
      };

      const rulesMax = [
        [
          '{a.output.data.num1}',
          '{a.output.data.num2}',
          '{a.output.data.num3}',
        ],
        ['{@math.max}'],
      ];
      const pipeMax = new Pipe(rulesMax, jobData);
      expect(pipeMax.process()).toBe(Math.max(5, 7, 3));
    });
  });

  //eventually add: number, math, object, array, util, string, date, regex, json, url, html, xml, csv, markdown, text, image, audio, video, file

  describe('process', () => {
    it('should chain multiple mapping transformations', () => {
      jobData = {
        a: {
          output: {
            data: {
              full_name: 'Luke Birdeau',
            },
          },
        },
      };

      rules = [
        ['{a.output.data.full_name}', ' '],
        ['{@string.split}', 0],
        ['{@array.get}'],
      ];

      pipe = new Pipe(rules, jobData);
      let result = pipe.process();

      expect(result).toEqual('Luke');

      rules = [
        ['["ke", "bab", "case"]'],
        ['{@json.parse}', '-'],
        ['{@array.join}'],
      ];

      pipe = new Pipe(rules, jobData);
      result = pipe.process();

      expect(result).toEqual('ke-bab-case');
    });

    it('should exec nested pipes', () => {
      jobData = {
        a: {
          output: {
            data: {
              workflowDimension: ',0,0,1',
              index: 13,
              duration: 1000,
            },
          },
        },
      };

      rules = [
        {
          '@pipe': [
            [
              '-sleep',
              '{a.output.data.workflowDimension}',
              '-',
              '{a.output.data.index}',
              '-',
            ],
            ['{@string.concat}'],
          ],
        },
        { '@pipe': [['{a.output.data.duration}']] },
        ['{@object.create}'],
      ];

      pipe = new Pipe(rules, jobData);
      const result = pipe.process();
      expect(result).toEqual({ '-sleep,0,0,1-13-': 1000 });
    });

    it('should reduce, split, map, create, push', () => {
      jobData = {
        a: {
          output: {
            data: [{ full_name: 'Luke Birdeau' }, { full_name: 'John Doe' }],
          },
        },
      };

      rules = [
        ['{a.output.data}', []],
        {
          '@reduce': [
            { '@pipe': [['{$output}']] },
            {
              '@pipe': [
                {
                  '@pipe': [['first']],
                },
                {
                  '@pipe': [
                    ['{$item.full_name}', ' '],
                    ['{@string.split}', 0],
                    ['{@array.get}'],
                  ],
                },
                {
                  '@pipe': [['last']],
                },
                {
                  '@pipe': [
                    ['{$item.full_name}', ' '],
                    ['{@string.split}', 1],
                    ['{@array.get}'],
                  ],
                },
                ['{@object.create}'],
              ],
            },
            ['{@array.push}'],
          ],
        },
      ];

      pipe = new Pipe(rules, jobData);
      const result = pipe.process();

      expect(result).toEqual([
        { first: 'Luke', last: 'Birdeau' },
        { first: 'John', last: 'Doe' },
      ]);
    });

    it('should reduce, split, map, create, concat, push', () => {
      jobData = {
        a: {
          output: {
            workflowDimension: ',0,0,1',
            index: 5,
            data: {
              '0': { type: 'wait', data: { first: 'Luke', last: 'Birdeau' } }, //index 5 (index + 0)
              '1': { type: 'sleep', data: { first: 'John', last: 'Doe' } }, //index 6 (index + 1)
            },
          },
        },
      };

      rules = [
        ['{a.output.data}', {}],
        {
          '@reduce': [
            { '@pipe': [['{$output}']] },
            {
              '@pipe': [
                { '@pipe': [['-']] },
                { '@pipe': [['{$item}', 'type'], ['{@object.get}']] },
                { '@pipe': [['{a.output.workflowDimension}']] },
                { '@pipe': [['-']] },
                {
                  '@pipe': [
                    { '@pipe': [['{a.output.index}']] },
                    { '@pipe': [['{$index}']] },
                    ['{@math.add}'],
                  ],
                },
                { '@pipe': [['-']] },
                ['{@string.concat}'],
              ],
            },
            { '@pipe': [['{$item}', 'data'], ['{@object.get}']] },
            ['{@object.set}'],
          ],
        },
      ];

      pipe = new Pipe(rules, jobData);
      const result = pipe.process();

      expect(result).toEqual({
        '-wait,0,0,1-5-': { first: 'Luke', last: 'Birdeau' },
        '-sleep,0,0,1-6-': { first: 'John', last: 'Doe' },
      });
    });
  });
});
