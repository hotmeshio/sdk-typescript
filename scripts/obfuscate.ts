import * as fs from 'fs';
import * as path from 'path';

import JavaScriptObfuscator from 'javascript-obfuscator';

const obfuscateTargets = [
  './build/modules/key',
  './build/modules/utils',
  './build/services/activities',
  './build/services/collator',
  './build/services/compiler',
  './build/services/engine',
  './build/services/exporter',
  './build/services/mapper',
  './build/services/meshflow/exporter',
  './build/services/pipe',
  './build/services/quorum',
  './build/services/reporter',
  './build/services/router',
  './build/services/serializer',
  './build/services/search/providers',
  './build/services/store/providers',
  './build/services/stream/providers',
  './build/services/sub/providers',
  './build/services/task',
  './build/services/telemetry',
  './build/services/worker',
];

const obfuscate = (filePath: string) => {
  const code = fs.readFileSync(filePath, 'utf8');
  const obfuscationResult = JavaScriptObfuscator.obfuscate(code, {
    compact: true, // Remove unnecessary spaces and newlines
    controlFlowFlattening: false, // Avoid inflating code size
    deadCodeInjection: false, // Avoid adding extra dead code
    debugProtection: false, // Disable debug protection
    disableConsoleOutput: false, // Keep console outputs unchanged
    identifierNamesGenerator: 'mangled', // Short variable names like 'a', 'b', 'c'
    log: false, // Suppress logs
    renameGlobals: false, // Avoid renaming global variables
    rotateStringArray: true, // Minimize string array impact
    selfDefending: false, // Avoid adding self-defending logic
    shuffleStringArray: true, // Reduce predictable patterns
    splitStrings: false, // Avoid splitting strings
    stringArray: true, // Group strings into an array
    stringArrayEncoding: [], // No encoding to keep size minimal
    stringArrayThreshold: 0.75, // Adjust threshold for optimal size
    transformObjectKeys: false, // Avoid inflating object keys
    unicodeEscapeSequence: false, // Disable Unicode escape sequences
  });

  fs.writeFileSync(filePath, obfuscationResult.getObfuscatedCode());
};

const obfuscateDir = (dir: string) => {
  const files = fs.readdirSync(dir);
  for (const file of files) {
    const filePath = path.join(dir, file);
    const stat = fs.statSync(filePath);

    if (stat.isDirectory()) {
      obfuscateDir(filePath);
    } else if (
      path.extname(file) === '.js' &&
      shouldObfuscate(`/app/${filePath}`)
    ) {
      obfuscate(filePath);
    }
  }
};

// Check if the file should be obfuscated
const shouldObfuscate = (filePath: string): boolean => {
  return obfuscateTargets.some((target) => {
    return filePath.startsWith(path.resolve(target));
  });
};

// Obfuscate only files in the specified folders or paths
obfuscateDir('./build');

console.log('Selective obfuscation complete.');
