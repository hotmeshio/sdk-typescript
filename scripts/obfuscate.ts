import * as fs from 'fs';
import * as path from 'path';

import { minify } from 'terser';
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

function shouldObfuscate(filePath) {
  return obfuscateTargets.some((target) =>
    path.resolve(filePath).startsWith(path.resolve(target)),
  );
}

async function processDir(dir) {
  const files = fs.readdirSync(dir);
  for (const file of files) {
    const filePath = path.join(dir, file);
    const stat = fs.statSync(filePath);
    if (stat.isDirectory()) {
      await processDir(filePath);
    } else if (path.extname(file) === '.js' && shouldObfuscate(filePath)) {
      await processFile(filePath);
    }
  }
}

async function processFile(filePath) {
  const code = fs.readFileSync(filePath, 'utf8');

  // First run Terser to minify and keep only JSDoc
  const terserResult = await minify(code, {
    compress: true,
    mangle: true,
    format: {
      beautify: false,
      comments: (node, comment) => {
        // Keep only JSDoc comments. JSDoc are block comments (comment2) that start with '*'
        // A typical JSDoc comment starts with `/**`
        return (
          comment.type === 'comment2' && comment.value.trim().startsWith('*')
        );
      },
    },
  });

  // Now run the obfuscator on the Terser output
  const obfuscationResult = JavaScriptObfuscator.obfuscate(
    terserResult.code as string,
    {
      compact: true,
      controlFlowFlattening: false,
      deadCodeInjection: false,
      debugProtection: false,
      disableConsoleOutput: false,
      identifierNamesGenerator: 'mangled',
      log: false,
      renameGlobals: false,
      rotateStringArray: true,
      selfDefending: false,
      shuffleStringArray: true,
      splitStrings: false,
      stringArray: true,
      stringArrayEncoding: [],
      stringArrayThreshold: 0.75,
      transformObjectKeys: false,
      unicodeEscapeSequence: false,
      // Don't filter comments here. All non-JSDoc are already removed by Terser.
      // The JSDoc comments that remain will be kept as is.
      comments: true,
    },
  );

  fs.writeFileSync(filePath, obfuscationResult.getObfuscatedCode());
}

(async () => {
  await processDir('./build');
  console.log('Selective obfuscation complete.');
})();
