# Formal models

The first [transport model](transport/README.md) uses Quint to check that an
established connection cannot remain permanently open on only one side, assuming
the scheduler continues running and old datagrams eventually expire. The model
is separate from the Go implementation; it is not a refinement proof of Go code.

## Quint syntax highlighting in VS Code

The [local extension](vscode-quint/package.json) associates `.qnt` files with
Quint. VS Code reads its [TextMate grammar](vscode-quint/syntaxes/quint.tmLanguage.json)
and editor configuration directly. The installed extension contains only JSON
and VSIX metadata.

Run both commands from the repository root in your VS Code terminal. Packaging
uses the Python 3 standard library and works offline.

```sh
python3 formal/vscode-quint/package_vsix.py
code --install-extension "$PWD/formal/build/quint-syntax-0.1.0.vsix"
```

You can also choose **Extensions: Install from VSIX...** in the Command Palette
and select the same file. Open `formal/transport/core.qnt` and check that the
language mode in the status bar reads **Quint**.

## Run from WSL/Linux

Use Node.js 22 or newer, npm, and Java on `PATH` (the initial run used OpenJDK 25).
Quint is pinned to 0.33.1 in the lockfile. Verification pins the Apalache 0.62.4
distribution, which Quint uses to translate to TLA+ and supply the TLC backend.
The first verification downloads that distribution into the ignored `.quint/`
directory and starts a local translation service on port 8822. Subsequent runs
reuse the download. Run verification commands sequentially.

```sh
cd formal
npm ci
npm run check
```

The npm scripts assume a POSIX shell, matching the repository's WSL development
environment. If the default npm cache is read-only, use
`npm --cache /tmp/wsl-proxy-npm-cache ci`.

| Command | Purpose |
| --- | --- |
| `npm run typecheck` | Parse, typecheck, and check effects for all instances |
| `npm test` | Run 12 deterministic fault scenarios |
| `npm run simulate` | Check safety on 1,000 seeded traces, at most 100 steps each |
| `npm run verify` | Exhaustively check the small instance with TLC, including liveness |
| `npm run counterexamples` | Require temporal counterexamples when idle expiry or scheduler fairness is removed |
| `npm run check` | Run all of the above |

Simulation uses Quint's TypeScript backend, so it needs no Rust executable.
Simulation and scenario tests do **not** establish liveness. TLC checks the
finite reachable graph, including infinite cycles and stuttering; the verification
command does not impose a trace-length bound. See the official
[model checker guide](https://quint.sh/docs/model-checkers) and
[CLI manual](https://quint.sh/docs/quint).

Negative checks save full TLC traces in `build/no-idle-expiry.log` and
`build/no-scheduler-fairness.log`. They pass only on an actual temporal violation
with a looping trace; a tool failure is an error. `build/`, tool downloads,
`node_modules/`, and generated checker output are ignored by Git.

Go validation remains independent: `make check` and
`make build-windows WINDOWS_ARCH=arm64` from the repository root.
