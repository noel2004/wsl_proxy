// A failed command alone is not evidence: require TLC's temporal violation and
// a lasso trace, so parse, download, JVM, and configuration errors fail this check.
import { spawnSync } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const quint = fileURLToPath(new URL('../node_modules/@informalsystems/quint/dist/src/cli.js', import.meta.url));
const cases = [
  ['no-idle-expiry', 'transport/no_idle.qnt', 'fair_no_linger'],
  ['no-scheduler-fairness', 'transport/transport.qnt', 'no_linger'],
];
mkdirSync(new URL('../build/', import.meta.url), { recursive: true });

for (const [name, model, property] of cases) {
  const result = spawnSync(process.execPath, [
    quint, 'verify', model, '--backend', 'tlc', '--apalache-version', '0.62.4',
    '--temporal', property, '--tlc-config', 'tlc.json', '--verbosity', '3',
  ], {
    cwd: root,
    env: { ...process.env, QUINT_HOME: `${root}.quint` },
    encoding: 'utf8',
    maxBuffer: 8 * 1024 * 1024,
  });
  const output = (result.stdout ?? '') + (result.stderr ?? '');
  writeFileSync(new URL(`../build/${name}.log`, import.meta.url), output);
  if (result.error || result.status !== 1 ||
      !/Error: Temporal propert(?:y .* was|ies were) violated/.test(output) ||
      !output.includes('error: found a counterexample') ||
      !/Back to state|Stuttering/.test(output)) {
    console.error(output);
    throw new Error(`${name}: expected a temporal counterexample, got ${result.error ?? result.status}`);
  }
  console.log(`${name}: expected temporal counterexample confirmed (build/${name}.log)`);
}
