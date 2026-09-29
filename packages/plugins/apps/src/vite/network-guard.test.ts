// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2019-Present Datadog, Inc.

/* global globalThis */

import child_process from 'child_process';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { promisify } from 'util';
import worker_threads from 'worker_threads';

import {
    forceReset,
    guardWorker,
    installGuardedProperty,
    getSharedContext,
    runAllowed,
    runBlocked,
    trustedFetch,
} from './network-guard';

// fs/child_process/worker_threads are real process-wide singletons — a test that leaves them patched leaks into later tests in the same worker.
afterEach(() => {
    forceReset();
});

// `(globalThis as { fetch: typeof fetch }).fetch = impl` repeated verbatim at every mock/restore
// call site — this collapses the cast to one place.
function setGlobalFetch(impl: typeof fetch): void {
    (globalThis as { fetch: typeof fetch }).fetch = impl;
}

describe('network-guard', () => {
    describe('runBlocked', () => {
        // spawn()/fork() synthesize a brand-new ChildProcess and never throw synchronously in real
        // Node — failure is only ever reported via the returned object's async 'error' event, so
        // the guard returns a stub shaped like the real return value instead of throwing.
        test("Should block child_process.spawn() and fork() made inside fn via the async 'error' event on the returned stub, not a synchronous throw", async () => {
            await runBlocked(async () => {
                let child: ReturnType<typeof child_process.spawn> | undefined;
                expect(() => {
                    child = child_process.spawn('curl', ['https://example.com']);
                }).not.toThrow();
                const err = await new Promise<Error>((resolve) => child?.once('error', resolve));
                expect(err.message).toMatch(/Spawning a subprocess is not allowed/);
            });

            await runBlocked(async () => {
                let child: ReturnType<typeof child_process.fork> | undefined;
                expect(() => {
                    child = child_process.fork('./some-script.js');
                }).not.toThrow();
                const err = await new Promise<Error>((resolve) => child?.once('error', resolve));
                expect(err.message).toMatch(/Spawning a subprocess is not allowed/);
            });
        });

        // Real spawn()/fork() always populate stdout/stderr/stdin and (for fork()) send()/
        // disconnect(), even for a command that never actually runs — a caller commonly touches
        // these right after the call, before any 'error' event has had a chance to fire.
        test('Should let a blocked spawn()/fork() stub be used like a real ChildProcess without throwing', async () => {
            await runBlocked(async () => {
                const child = child_process.spawn('curl', ['https://example.com']);
                expect(() => child.stdout?.on('data', () => {})).not.toThrow();
                expect(() => child.stderr?.on('data', () => {})).not.toThrow();
                expect(() => child.stdin?.write('data')).not.toThrow();
                expect(child.kill()).toBe(false);
            });

            await runBlocked(async () => {
                const child = child_process.fork('./some-script.js');
                expect(() => child.disconnect()).not.toThrow();
                // send() with a callback: the callback receives the error, matching a real
                // disconnected channel's contract.
                const callbackErr = await new Promise<Error>((resolve) => {
                    expect(() =>
                        child.send({ hello: 'world' }, (err) => resolve(err as Error)),
                    ).not.toThrow();
                });
                expect(callbackErr.message).toBeTruthy();

                // send() with no callback: falls back to an 'error' event instead of silently
                // dropping the failure.
                const eventErr = await new Promise<Error>((resolve) => {
                    child.once('error', resolve);
                    expect(() => child.send({ hello: 'world' })).not.toThrow();
                });
                expect(eventErr.message).toBeTruthy();
            });
        });

        // spawnSync never throws in real Node either — it returns a SpawnSyncReturns-shaped object
        // with `.error` set, so the guard mirrors that shape instead of throwing. `output` is `null`
        // on a real launch failure (not an array), and `stdout`/`stderr` are `undefined` — a caller
        // checking `if (result.output) { result.output[1].toString() }` would TypeError against a
        // truthy-but-empty array.
        test('Should block child_process.spawnSync() made inside fn via a SpawnSyncReturns-shaped `.error` matching real Node exactly, not a synchronous throw', async () => {
            await runBlocked(async () => {
                let result: ReturnType<typeof child_process.spawnSync> | undefined;
                expect(() => {
                    result = child_process.spawnSync('curl', ['https://example.com']);
                }).not.toThrow();
                expect(result?.error?.message).toMatch(/Spawning a subprocess is not allowed/);
                expect(result?.output).toBeNull();
                expect(result?.stdout).toBeUndefined();
                expect(result?.stderr).toBeUndefined();
                expect(result?.status).toBeNull();
                expect(result?.signal).toBeNull();
            });
        });

        // exec/execFile report failure via an error-first callback in real Node, unlike execSync/
        // execFileSync below, which genuinely do throw synchronously. Real Node sets stdout/stderr
        // to empty strings (not undefined) even on a launch failure — a caller doing
        // `err.stderr.trim()` in its callback would TypeError against `undefined`.
        test('Should block child_process.exec() and execFile() made inside fn via their error-first callback, matching real Node exactly', async () => {
            await runBlocked(async () => {
                const [err, stdout, stderr] = await new Promise<[Error, unknown, unknown]>(
                    (resolve) => {
                        expect(() =>
                            child_process.exec('curl https://example.com', (execErr, out, errOut) =>
                                resolve([execErr as Error, out, errOut]),
                            ),
                        ).not.toThrow();
                    },
                );
                expect(err.message).toMatch(/Spawning a subprocess is not allowed/);
                expect(stdout).toBe('');
                expect(stderr).toBe('');
            });

            await runBlocked(async () => {
                const [err, stdout, stderr] = await new Promise<[Error, unknown, unknown]>(
                    (resolve) => {
                        expect(() =>
                            child_process.execFile(
                                'curl',
                                ['https://example.com'],
                                (execErr, out, errOut) => resolve([execErr as Error, out, errOut]),
                            ),
                        ).not.toThrow();
                    },
                );
                expect(err.message).toMatch(/Spawning a subprocess is not allowed/);
                expect(stdout).toBe('');
                expect(stderr).toBe('');
            });
        });

        test('Should not crash the process when exec()/execFile() is called with no callback', async () => {
            await runBlocked(async () => {
                expect(() => child_process.exec('curl https://example.com')).not.toThrow();
                expect(() => child_process.execFile('curl', ['https://example.com'])).not.toThrow();
            });
            // If the guard had emitted an unlistened 'error' on the discarded stub, the resulting
            // uncaught exception would already have crashed this Jest worker by now.
            await new Promise((resolve) => setImmediate(resolve));
        });

        test('Should block child_process.execSync() and execFileSync() made inside fn via a synchronous throw, matching their real contract', async () => {
            await expect(
                runBlocked(async () => {
                    child_process.execSync('curl https://example.com');
                }),
            ).rejects.toThrow(/Spawning a subprocess is not allowed/);
            await expect(
                runBlocked(async () => {
                    child_process.execFileSync('curl', ['https://example.com']);
                }),
            ).rejects.toThrow(/Spawning a subprocess is not allowed/);
        });

        // promisify.custom lives on the specific function object, not inherited by a fresh wrapper — @dd/tools execute() depends on the real shape.
        test('Should resolve promisify(execFile) to the real {stdout, stderr} shape, not a bare string, when not blocked', async () => {
            const execFileP = promisify(child_process.execFile);
            const result = await execFileP('node', ['-e', 'console.log("hi")']);
            expect(result).toEqual(
                expect.objectContaining({ stdout: expect.stringContaining('hi') }),
            );
        });

        test("Should still block promisify(execFile) inside a runBlocked scope, with stdout/stderr matching real Node's empty-string contract", async () => {
            const execFileP = promisify(child_process.execFile);
            await expect(
                runBlocked(async () => {
                    await execFileP('node', ['-e', 'console.log("hi")']);
                }),
            ).rejects.toMatchObject({
                message: expect.stringMatching(/Spawning a subprocess is not allowed/),
                stdout: '',
                stderr: '',
            });
        });

        // exec/execFile share a guard maker but take different argument shapes — a fix for one could silently miss the other.
        test('Should resolve promisify(exec) to the real {stdout, stderr} shape and still block it inside runBlocked', async () => {
            const execP = promisify(child_process.exec);
            const result = await execP('node -e "console.log(\'hi\')"');
            expect(result).toEqual(
                expect.objectContaining({ stdout: expect.stringContaining('hi') }),
            );

            await expect(
                runBlocked(async () => {
                    await execP('node -e "console.log(\'hi\')"');
                }),
            ).rejects.toThrow(/Spawning a subprocess is not allowed/);
        });

        // Matches Node's real promisify(execFile) contract: a rejected error carries stdout/stderr too, not just a resolved success.
        test('Should attach stdout/stderr onto a rejected promisify(execFile) error, matching real Node behavior', async () => {
            const execFileP = promisify(child_process.execFile);
            await expect(
                execFileP('node', [
                    '-e',
                    'console.log("out"); console.error("boom"); process.exit(1)',
                ]),
            ).rejects.toEqual(
                expect.objectContaining({
                    stdout: expect.stringContaining('out'),
                    stderr: expect.stringContaining('boom'),
                }),
            );
        });

        // Matches Node's real PromiseWithChild contract — a caller outside a blocked scope that
        // inspects/signals/terminates `.child` must not lose it to this guard's own implementation.
        test("Should expose the spawned ChildProcess as `.child` on promisify(execFile)'s returned promise", async () => {
            const execFileP = promisify(child_process.execFile);
            const resultPromise = execFileP('node', ['-e', 'console.log("hi")']);
            expect(resultPromise.child).toBeInstanceOf(child_process.ChildProcess);
            await resultPromise;
        });

        test("Should expose the spawned ChildProcess as `.child` on promisify(exec)'s returned promise too", async () => {
            const execP = promisify(child_process.exec);
            const resultPromise = execP('node -e "console.log(\'hi\')"');
            expect(resultPromise.child).toBeInstanceOf(child_process.ChildProcess);
            await resultPromise;
        });

        // ChildProcess.prototype.spawn isn't in @types/node's public surface, so a locally-scoped
        // interface stands in for its real shape instead of an `any` escape hatch.
        interface ChildProcessWithSpawn {
            spawn(options: { file: string }): number;
            once(event: 'error', listener: (err: Error) => void): void;
        }

        // A dependency calling `new child_process.ChildProcess().spawn(...)` directly bypasses all
        // the higher-level guarded factory functions above. Unlike those, `this` is already the
        // real ChildProcess instance — spawn() itself never throws in real Node and returns a
        // synchronous integer, not undefined, so the guard emits 'error' on `this` and returns a
        // negative placeholder rather than fabricating a stub.
        test("Should block a direct new child_process.ChildProcess().spawn(...) call via the async 'error' event, bypassing the factory functions", async () => {
            await runBlocked(async () => {
                const child = new child_process.ChildProcess() as unknown as ChildProcessWithSpawn;
                let returnValue: number | undefined;
                expect(() => {
                    returnValue = child.spawn({ file: 'curl' });
                }).not.toThrow();
                expect(typeof returnValue).toBe('number');
                expect(returnValue).toBeLessThan(0);
                const err = await new Promise<Error>((resolve) => child.once('error', resolve));
                expect(err.message).toMatch(/Spawning a subprocess is not allowed/);
            });
        });

        // A worker gets a fresh V8 realm with its own module registry, so nothing inside it inherits
        // this file's monkeypatches — the only enforceable boundary is blocking construction itself.
        test('Should block new Worker(...) construction made inside fn', async () => {
            await expect(
                runBlocked(async () => {
                    new worker_threads.Worker('', { eval: true });
                }),
            ).rejects.toThrow(/Spawning a worker thread is not allowed/);
        });

        test('Should allow constructing, messaging, and cleanly terminating a Worker outside a blocked scope', async () => {
            const worker = new worker_threads.Worker(
                "require('worker_threads').parentPort.on('message', () => undefined);",
                { eval: true },
            );
            expect(worker).toBeInstanceOf(worker_threads.Worker);
            try {
                expect(() => worker.postMessage('ping')).not.toThrow();
            } finally {
                await expect(worker.terminate()).resolves.toEqual(expect.any(Number));
            }
        });

        // fn returning doesn't mean fn is done — detached async work it scheduled without awaiting keeps running and must still see the guard.
        test('Should still block a detached, unawaited setTimeout callback scheduled during fn, even after fn itself has already resolved', async () => {
            const probePath = path.join(os.tmpdir(), 'dd-network-guard-probe-detached.txt');
            let detachedWriteResult: Promise<unknown> | undefined;
            let detachedWriteSettled = false;

            await runBlocked(async () => {
                // Deliberately not awaited — fn returns immediately while this keeps running in the background.
                setTimeout(() => {
                    const result = fs.promises.writeFile(probePath, 'data');
                    detachedWriteResult = result;
                    // Attached synchronously so the rejection is never briefly unhandled before the `.rejects` assertion below attaches its own handler.
                    result.then(
                        () => {
                            detachedWriteSettled = true;
                        },
                        () => {
                            detachedWriteSettled = true;
                        },
                    );
                }, 0);
            });

            // fn (and therefore runBlocked) has already resolved here — a per-cycle restore would have put the real write method back before this fires.
            await new Promise((resolve) => setTimeout(resolve, 10));

            expect(detachedWriteSettled).toBe(true);
            await expect(detachedWriteResult).rejects.toThrow(
                /Writing to the filesystem is not allowed/,
            );
        });

        test('Should restore the real fs.writeFileSync after fn resolves', async () => {
            const realWriteFileSync = fs.writeFileSync;
            await runBlocked(async () => undefined);
            expect(fs.writeFileSync).toBe(realWriteFileSync);
        });

        test('Should restore the real fs.writeFileSync even when fn throws', async () => {
            const realWriteFileSync = fs.writeFileSync;
            await expect(
                runBlocked(async () => {
                    throw new Error('customer function boom');
                }),
            ).rejects.toThrow('customer function boom');
            expect(fs.writeFileSync).toBe(realWriteFileSync);
        });

        test('Should not block a subsequent, separate runBlocked call after an earlier one already restored', async () => {
            await expect(
                runBlocked(async () => {
                    throw new Error('first execution boom');
                }),
            ).rejects.toThrow('first execution boom');

            // Confirms the guard doesn't leak a "still blocked" state the way a naive boolean (never reset on throw) could.
            const result = await runBlocked(async () => 'second execution result');
            expect(result).toBe('second execution result');
        });

        // The guarded property holds no snapshot to reinstall — its setter just updates the delegate — so an idle forceReset() has nothing to clobber.
        test('Should make an idle forceReset() a true no-op, never reinstalling an earlier mock over the current one', async () => {
            const originalWriteFileSync = fs.writeFileSync;
            try {
                const mockA = jest.fn().mockReturnValue('mock A');
                (fs as unknown as { writeFileSync: unknown }).writeFileSync = mockA;

                await runBlocked(async () => undefined);
                expect(fs.writeFileSync('probe.txt', 'data')).toBe('mock A');

                // A later, unrelated mock is installed with runBlocked never called again in between, so the guard is genuinely idle.
                const mockB = jest.fn().mockReturnValue('mock B');
                (fs as unknown as { writeFileSync: unknown }).writeFileSync = mockB;

                forceReset();

                expect(fs.writeFileSync('probe.txt', 'data')).toBe('mock B');
            } finally {
                (fs as unknown as { writeFileSync: unknown }).writeFileSync = originalWriteFileSync;
            }
        });

        // An abandoned execution's late settlement must not restore real fs access out from under a newer, active runBlocked scope.
        test("Should not let an abandoned runBlocked call's late restore corrupt a newer, currently-active runBlocked scope", async () => {
            const probePath = path.join(os.tmpdir(), 'dd-network-guard-probe-abandoned.txt');
            let resolveAbandoned: (() => void) | undefined;
            const abandoned = runBlocked(
                () =>
                    new Promise<void>((resolve) => {
                        resolveAbandoned = resolve;
                    }),
            );

            // Simulates the timeout handler abandoning this execution, exactly like local-execution.ts's timer callback.
            forceReset();

            // A second, newer execution starts its own scope; the write check runs from inside its fn to verify customer code is still blocked.
            let openGate: (() => void) | undefined;
            const gate = new Promise<void>((resolve) => {
                openGate = resolve;
            });
            let currentWriteResult: Promise<unknown> | undefined;
            const current = runBlocked(async () => {
                await gate;
                currentWriteResult = fs.promises.writeFile(probePath, 'data');
                await currentWriteResult.catch(() => undefined);
            });

            // The abandoned execution's fn() finally settles — its own finally block must not unblock the still-running newer scope.
            resolveAbandoned?.();
            await abandoned;

            openGate?.();
            await current;
            await expect(currentWriteResult).rejects.toThrow(
                /Writing to the filesystem is not allowed/,
            );
        });

        // The "const original = x; x = mock; x = original;" idiom hands the guard itself back on
        // restore — confirms this round-trips to the real value instead of recursing into itself.
        test('Should not infinite-recurse when a caller restores a previously-read guard back onto a guarded property', async () => {
            const nativeStandIn = jest.fn().mockReturnValue('native result');
            const originalWriteFileSync = fs.writeFileSync;
            (fs as unknown as { writeFileSync: unknown }).writeFileSync = nativeStandIn;

            try {
                const capturedOriginal = fs.writeFileSync;
                const mock = jest.fn().mockReturnValue('mock result');
                (fs as unknown as { writeFileSync: unknown }).writeFileSync = mock;

                expect(fs.writeFileSync('probe.txt', 'data')).toBe('mock result');

                (fs as unknown as { writeFileSync: unknown }).writeFileSync = capturedOriginal;

                expect(fs.writeFileSync('probe.txt', 'data')).toBe('native result');
            } finally {
                (fs as unknown as { writeFileSync: unknown }).writeFileSync = originalWriteFileSync;
            }
        });

        // The guarded property is a process-wide singleton — code that never entered any runBlocked scope must not be blocked by an unrelated one.
        test('Should not block a concurrent fs.writeFileSync made from code that never entered any runBlocked scope', async () => {
            const writeFileSyncMock = jest.fn().mockReturnValue('unrelated result');
            const originalWriteFileSync = fs.writeFileSync;
            (fs as unknown as { writeFileSync: unknown }).writeFileSync = writeFileSyncMock;

            try {
                let resolveBlocked: (() => void) | undefined;
                const blocked = runBlocked(
                    () =>
                        new Promise<void>((resolve) => {
                            resolveBlocked = resolve;
                        }),
                );

                // Made from code entirely outside runBlocked/runAllowed, e.g. a concurrent cloud-mode request's own real write call.
                expect(fs.writeFileSync('unrelated.txt', 'data')).toBe('unrelated result');

                resolveBlocked?.();
                await blocked;
            } finally {
                (fs as unknown as { writeFileSync: unknown }).writeFileSync = originalWriteFileSync;
            }
        });
    });

    describe('runAllowed', () => {
        test('Should let a real fs write through when nested inside runBlocked', async () => {
            const writeFileSyncMock = jest.fn().mockReturnValue('real result');
            const originalWriteFileSync = fs.writeFileSync;
            (fs as unknown as { writeFileSync: unknown }).writeFileSync = writeFileSyncMock;

            try {
                const result = await runBlocked(async () =>
                    runAllowed(async () => fs.writeFileSync('allowed.txt', 'data')),
                );
                expect(result).toBe('real result');
                expect(writeFileSyncMock).toHaveBeenCalledWith('allowed.txt', 'data');
            } finally {
                (fs as unknown as { writeFileSync: unknown }).writeFileSync = originalWriteFileSync;
            }
        });

        test('Should re-block fs writes once the allowed call finishes, while the outer execution is still running', async () => {
            const probePath = path.join(os.tmpdir(), 'dd-network-guard-probe-reblock.txt');
            await runBlocked(async () => {
                await runAllowed(async () => undefined);
                await expect(fs.promises.writeFile(probePath, 'data')).rejects.toThrow(
                    /Writing to the filesystem is not allowed/,
                );
            });
        });

        test('Should keep two concurrent, legitimate $.Actions calls both allowed while they overlap, independently of each other', async () => {
            const writeFileSyncMock = jest.fn().mockReturnValue(undefined);
            const originalWriteFileSync = fs.writeFileSync;
            (fs as unknown as { writeFileSync: unknown }).writeFileSync = writeFileSyncMock;
            const order: string[] = [];

            try {
                await runBlocked(async () => {
                    const first = runAllowed(async () => {
                        order.push('first-start');
                        await new Promise((r) => setTimeout(r, 20));
                        // Must still succeed even after `second` already finished — each call's exemption is scoped to its own async chain, not a shared depth counter.
                        expect(() => fs.writeFileSync('first.txt', 'data')).not.toThrow();
                        order.push('first-end');
                    });
                    const second = runAllowed(async () => {
                        order.push('second-start');
                        expect(() => fs.writeFileSync('second.txt', 'data')).not.toThrow();
                        order.push('second-end');
                    });

                    await second;
                    await first;
                });
            } finally {
                (fs as unknown as { writeFileSync: unknown }).writeFileSync = originalWriteFileSync;
            }

            expect(order).toEqual(['first-start', 'second-start', 'second-end', 'first-end']);
        });

        // A shared, process-wide "allowed" toggle would wrongly let this sibling write through while an unrelated $.Actions call is in flight.
        test('Should keep a sibling raw fs.writeFileSync call blocked while a concurrent, legitimate $.Actions call is in flight', async () => {
            const writeFileSyncMock = jest.fn().mockReturnValue('real result');
            const originalWriteFileSync = fs.writeFileSync;
            (fs as unknown as { writeFileSync: unknown }).writeFileSync = writeFileSyncMock;

            try {
                await runBlocked(async () => {
                    const allowedCall = runAllowed(async () => {
                        await new Promise((r) => setTimeout(r, 20));
                        return fs.writeFileSync('allowed.txt', 'data');
                    });

                    // Made directly by "customer code", not through runAllowed, while allowedCall is still in flight.
                    expect(() => fs.writeFileSync('sibling.txt', 'data')).toThrow(
                        /Writing to the filesystem is not allowed/,
                    );

                    await expect(allowedCall).resolves.toBe('real result');
                });
            } finally {
                (fs as unknown as { writeFileSync: unknown }).writeFileSync = originalWriteFileSync;
            }
        });

        test('Should still re-block after the allowed call finishes even if it throws', async () => {
            const probePath = path.join(os.tmpdir(), 'dd-network-guard-probe-reblock-throw.txt');
            await runBlocked(async () => {
                await expect(
                    runAllowed(async () => {
                        throw new Error('action call failed');
                    }),
                ).rejects.toThrow('action call failed');
                await expect(fs.promises.writeFile(probePath, 'data')).rejects.toThrow(
                    /Writing to the filesystem is not allowed/,
                );
            });
        });

        // An abandoned execution's in-flight $.Actions call settling late must not affect any execution that runs afterward.
        test("Should not let an abandoned runAllowed call's late settlement affect later executions", async () => {
            const probePath = path.join(os.tmpdir(), 'dd-network-guard-probe-abandoned-action.txt');
            let resolveAbandonedAction: (() => void) | undefined;
            const abandonedAction = runAllowed(
                () =>
                    new Promise<void>((resolve) => {
                        resolveAbandonedAction = resolve;
                    }),
            );

            // Simulates the timeout handler abandoning this execution while the $.Actions call above is still in flight.
            forceReset();

            // A newer execution's own legitimate $.Actions call must be correctly allowed through and re-blocked afterward.
            const result = await runBlocked(async () => {
                await runAllowed(async () => 'newer allowed call');
                await expect(fs.promises.writeFile(probePath, 'data')).rejects.toThrow(
                    /Writing to the filesystem is not allowed/,
                );
                return 'newer execution result';
            });
            expect(result).toBe('newer execution result');

            // The abandoned call finally settles, well after being superseded — it must not affect anything else.
            resolveAbandonedAction?.();
            await abandonedAction;

            // A further, unrelated later execution's own $.Actions call must still work.
            const laterResult = await runBlocked(async () =>
                runAllowed(async () => 'later allowed call'),
            );
            expect(laterResult).toBe('later allowed call');
        });

        // Stricter than the test above: runAllowed is called after forceReset already cleared the guard, so it must be a no-op.
        test('Should treat a runAllowed call that only starts after its execution was already abandoned as a no-op, not a stale-but-matching generation', async () => {
            const probePath = path.join(os.tmpdir(), 'dd-network-guard-probe-late-noop.txt');
            const writeFileSyncMock = jest.fn().mockReturnValue('ok');
            const originalWriteFileSync = fs.writeFileSync;
            (fs as unknown as { writeFileSync: unknown }).writeFileSync = writeFileSyncMock;

            try {
                forceReset();

                let resolveLateAction: (() => void) | undefined;
                const lateAction = runAllowed(
                    () =>
                        new Promise<void>((resolve) => {
                            resolveLateAction = resolve;
                        }),
                );
                resolveLateAction?.();
                await lateAction;

                // If the bug were present, the late call's finally would have left writes permanently blocked even with nothing legitimate currently executing.
                expect(fs.writeFileSync('probe.txt', 'data')).toBe('ok');

                // A real, later execution must still work normally afterward.
                const result = await runBlocked(async () => {
                    await runAllowed(async () => undefined);
                    await expect(fs.promises.writeFile(probePath, 'data')).rejects.toThrow(
                        /Writing to the filesystem is not allowed/,
                    );
                    return 'later execution result';
                });
                expect(result).toBe('later execution result');
            } finally {
                (fs as unknown as { writeFileSync: unknown }).writeFileSync = originalWriteFileSync;
            }
        });

        // Regression test: forceReset()'s unconditional reset would have wrongly cleared a newer,
        // still-active scope here too — abandonIfCurrent() must only clear its own scope.
        test("Should not let an abandoned execution's own scope handle disturb a newer, still-active execution when abandoned late", async () => {
            const probePath = path.join(os.tmpdir(), 'dd-network-guard-probe-late-abandon.txt');
            let abandonedScopeHandle: { abandonIfCurrent: () => void } | undefined;
            let resolveAbandonedFn: (() => void) | undefined;
            const abandoned = runBlocked(
                () =>
                    new Promise<void>((resolve) => {
                        resolveAbandonedFn = resolve;
                    }),
                (handle) => {
                    abandonedScopeHandle = handle;
                },
            );

            // A newer execution starts before the abandoned one's timeout fires, taking over as the active scope.
            let resolveAllowedCall: ((value: string) => void) | undefined;
            const newerExecution = runBlocked(async () => {
                const allowedResult = await runAllowed(
                    () =>
                        new Promise<string>((resolve) => {
                            resolveAllowedCall = resolve;
                        }),
                );
                await expect(fs.promises.writeFile(probePath, 'data')).rejects.toThrow(
                    /Writing to the filesystem is not allowed/,
                );
                return allowedResult;
            });

            // The abandoned execution's timeout fires here, after the newer scope has already taken over.
            abandonedScopeHandle?.abandonIfCurrent();

            resolveAllowedCall?.('newer allowed call, unaffected by the late abandon');
            await expect(newerExecution).resolves.toBe(
                'newer allowed call, unaffected by the late abandon',
            );

            resolveAbandonedFn?.();
            await abandoned;
        });

        // Only the write side is guarded (see network-guard.ts's comment above the fs write-guard
        // installs for why reads stay open, and for the graceful-fs collision that keeps
        // writeFile/appendFile/copyFile's callback form and chown/chmod out of this list entirely).
        describe('fs write guard', () => {
            let tmpDir: string;
            let testFile: string;

            beforeEach(() => {
                tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dd-fs-guard-'));
                testFile = path.join(tmpDir, 'test.txt');
            });

            afterEach(() => {
                fs.rmSync(tmpDir, { recursive: true, force: true });
            });

            test('Should block fs.writeFileSync made inside fn', async () => {
                await expect(
                    runBlocked(async () => {
                        fs.writeFileSync(testFile, 'data');
                    }),
                ).rejects.toThrow(/Writing to the filesystem is not allowed/);
                expect(fs.existsSync(testFile)).toBe(false);
            });

            // unlink reports failure via its mandatory error-first callback, never a synchronous throw.
            test('Should block fs.unlink made inside fn via its error-first callback, not a synchronous throw', async () => {
                fs.writeFileSync(testFile, 'data');
                await runBlocked(async () => {
                    const err = await new Promise<Error>((resolve) => {
                        expect(() =>
                            fs.unlink(testFile, (unlinkErr) => resolve(unlinkErr as Error)),
                        ).not.toThrow();
                    });
                    expect(err.message).toMatch(/Writing to the filesystem is not allowed/);
                });
                expect(fs.existsSync(testFile)).toBe(true);
            });

            // Real fs.unlink throws synchronously for a missing callback regardless of block
            // state, since `this` is the fs module object rather than an EventEmitter — a caller
            // bug that must surface as a thrown error, not be silently swallowed.
            test('Should throw synchronously from fs.unlink made inside fn with no callback argument', async () => {
                const unlinkWithoutCallback = fs.unlink as unknown as (path: string) => void;
                await expect(
                    runBlocked(async () => {
                        unlinkWithoutCallback(testFile);
                    }),
                ).rejects.toThrow(/Callback must be a function/);
            });

            test('Should reject rather than throw synchronously from fs.promises.writeFile when blocked', async () => {
                await runBlocked(async () => {
                    await expect(fs.promises.writeFile(testFile, 'data')).rejects.toThrow(
                        /Writing to the filesystem is not allowed/,
                    );
                });
                expect(fs.existsSync(testFile)).toBe(false);
            });

            test('Should allow fs.writeFileSync outside a blocked scope', () => {
                expect(() => fs.writeFileSync(testFile, 'data')).not.toThrow();
                expect(fs.readFileSync(testFile, 'utf8')).toBe('data');
            });

            test('Should still allow fs.readFileSync inside a blocked scope, since only writes are guarded', async () => {
                fs.writeFileSync(testFile, 'data');
                await runBlocked(async () => {
                    expect(fs.readFileSync(testFile, 'utf8')).toBe('data');
                });
            });

            // openSync itself stays allowed (documented residual gap), but write/writeSync on the
            // resulting fd is the far more direct path to the same disk write writeFileSync blocks
            // above — leaving it unguarded would make openSync+writeSync a trivial full bypass.
            test('Should block fs.writeSync made inside fn on an fd from openSync', async () => {
                const fd = fs.openSync(testFile, 'w');
                try {
                    await expect(
                        runBlocked(async () => {
                            fs.writeSync(fd, 'data');
                        }),
                    ).rejects.toThrow(/Writing to the filesystem is not allowed/);
                } finally {
                    fs.closeSync(fd);
                }
                expect(fs.readFileSync(testFile, 'utf8')).toBe('');
            });

            // Same openSync-then-fd-op bypass as writeSync above, but destroying existing content
            // instead of appending new content.
            test('Should block fs.ftruncateSync made inside fn on an fd from openSync', async () => {
                fs.writeFileSync(testFile, 'data');
                const fd = fs.openSync(testFile, 'r+');
                try {
                    await expect(
                        runBlocked(async () => {
                            fs.ftruncateSync(fd, 0);
                        }),
                    ).rejects.toThrow(/Writing to the filesystem is not allowed/);
                } finally {
                    fs.closeSync(fd);
                }
                expect(fs.readFileSync(testFile, 'utf8')).toBe('data');
            });

            test('Should block fs.write made inside fn via its error-first callback, not a synchronous throw', async () => {
                const fd = fs.openSync(testFile, 'w');
                try {
                    await runBlocked(async () => {
                        const err = await new Promise<Error>((resolve) => {
                            expect(() =>
                                fs.write(fd, 'data', (writeErr) => resolve(writeErr as Error)),
                            ).not.toThrow();
                        });
                        expect(err.message).toMatch(/Writing to the filesystem is not allowed/);
                    });
                } finally {
                    fs.closeSync(fd);
                }
                expect(fs.readFileSync(testFile, 'utf8')).toBe('');
            });

            test('Should block fs.ftruncate made inside fn via its error-first callback, not a synchronous throw', async () => {
                fs.writeFileSync(testFile, 'data');
                const fd = fs.openSync(testFile, 'r+');
                try {
                    await runBlocked(async () => {
                        const err = await new Promise<Error>((resolve) => {
                            expect(() =>
                                fs.ftruncate(fd, 0, (truncateErr) => resolve(truncateErr as Error)),
                            ).not.toThrow();
                        });
                        expect(err.message).toMatch(/Writing to the filesystem is not allowed/);
                    });
                } finally {
                    fs.closeSync(fd);
                }
                expect(fs.readFileSync(testFile, 'utf8')).toBe('data');
            });

            // mkdtemp creates a real directory, same class of write as mkdir — omitting it would let a
            // blocked function still create directories under the OS temp path.
            test('Should block fs.mkdtempSync made inside fn', async () => {
                await expect(
                    runBlocked(async () => {
                        fs.mkdtempSync(path.join(tmpDir, 'nested-'));
                    }),
                ).rejects.toThrow(/Writing to the filesystem is not allowed/);
                expect(fs.readdirSync(tmpDir)).toHaveLength(0);
            });

            test('Should block fs.mkdtemp made inside fn via its error-first callback, not a synchronous throw', async () => {
                await runBlocked(async () => {
                    const err = await new Promise<Error>((resolve) => {
                        expect(() =>
                            fs.mkdtemp(path.join(tmpDir, 'nested-'), (mkdtempErr) =>
                                resolve(mkdtempErr as Error),
                            ),
                        ).not.toThrow();
                    });
                    expect(err.message).toMatch(/Writing to the filesystem is not allowed/);
                });
                expect(fs.readdirSync(tmpDir)).toHaveLength(0);
            });

            test('Should reject rather than throw synchronously from fs.promises.mkdtemp when blocked', async () => {
                await runBlocked(async () => {
                    await expect(fs.promises.mkdtemp(path.join(tmpDir, 'nested-'))).rejects.toThrow(
                        /Writing to the filesystem is not allowed/,
                    );
                });
                expect(fs.readdirSync(tmpDir)).toHaveLength(0);
            });
        });
    });
});

describe('installGuardedProperty resilience', () => {
    // A wrapper closure over the previous guard (some mocking libraries' pattern, distinct from the
    // direct-reassignment case the WeakMap handles) would otherwise recurse into itself forever,
    // since its captured getReal() would read the shared `real` variable the new guard just set.
    test('Should not recurse when a guard is restored via a wrapper closure instead of direct reassignment', () => {
        const originalWriteFileSync = fs.writeFileSync;
        try {
            const realMock = jest.fn().mockReturnValue('real result');
            (fs as unknown as { writeFileSync: unknown }).writeFileSync = realMock;
            const previous = fs.writeFileSync;

            (fs as unknown as { writeFileSync: unknown }).writeFileSync = ((
                ...args: Parameters<typeof fs.writeFileSync>
            ) => previous(...args)) as typeof fs.writeFileSync;

            expect(fs.writeFileSync('probe.txt', 'data')).toBe('real result');
        } finally {
            (fs as unknown as { writeFileSync: unknown }).writeFileSync = originalWriteFileSync;
        }
    });
});

describe('installGuardedProperty security', () => {
    // A dependency could otherwise call `Object.defineProperty(target, 'value', {...})` directly to
    // replace the whole descriptor, silently restoring the real function — closed by installing
    // non-configurable. Unaffected by RUNNING_UNDER_JEST: shouldAllowConfigurableUnderJest only
    // special-cases globalThis, so a plain object target is non-configurable regardless of
    // environment — the globalThis-under-Jest carve-out has its own test below.
    test('Should make a guarded property non-configurable, closing the Object.defineProperty bypass, while still allowing plain reassignment', () => {
        const target: { value: unknown } = { value: () => 'real' };
        installGuardedProperty(
            target,
            'value',
            (getReal: () => () => unknown) =>
                (...args: unknown[]) =>
                    (getReal() as (...a: unknown[]) => unknown)(...args),
        );

        // A dependency replacing the whole descriptor outright must now fail loudly...
        expect(() => {
            Object.defineProperty(target, 'value', {
                configurable: true,
                enumerable: true,
                value: () => 'hostile replacement',
            });
        }).toThrow(/Cannot redefine property/);

        // ...while the legitimate "capture original, mock, restore" idiom still works via plain assignment.
        const mock = () => 'mocked';
        (target as { value: unknown }).value = mock;
        expect((target.value as () => string)()).toBe('mocked');
    });

    // The one target where installGuardedProperty deliberately becomes configurable is globalThis
    // under Jest, to survive Jest's own globalThis Proxy. Outside Jest, globalThis must get the same
    // non-configurable treatment as every other target — spawned as a real non-Jest process since
    // jest.isolateModules can't hide the real jest/describe/expect globals Jest injects for the file.
    test('Should make a guarded property on globalThis non-configurable in a real non-Jest process, closing the same bypass there', () => {
        const result = child_process
            .execFileSync(
                process.execPath,
                [
                    '-r',
                    'ts-node/register',
                    '-e',
                    "const ng = require(process.argv[1]); globalThis.__ngTestGlobalProp = () => 'real'; ng.installGuardedProperty(globalThis, '__ngTestGlobalProp', (getReal) => (...a) => getReal()(...a)); try { Object.defineProperty(globalThis, '__ngTestGlobalProp', { configurable: true, value: () => 'hostile' }); console.log('configurable'); } catch { console.log('non-configurable'); }",
                    require.resolve('./network-guard'),
                ],
                {
                    encoding: 'utf8',
                    env: {
                        ...process.env,
                        TS_NODE_TRANSPILE_ONLY: '1',
                        TS_NODE_COMPILER_OPTIONS: '{"module":"commonjs","moduleResolution":"node"}',
                    },
                },
            )
            .trim();

        expect(result).toBe('non-configurable');
    });

    // The configurable-relax retry in installGuardedProperty's catch branch exists only for
    // globalThis (Jest's globalThis Proxy). Every other target must keep failing loudly on a
    // defineProperty collision instead of silently downgrading to configurable: true.
    test('Should rethrow rather than silently relax configurability when defineProperty fails on a non-globalThis target', () => {
        const target: { value: unknown } = { value: () => 'real' };
        Object.defineProperty(target, 'value', {
            value: () => 'real',
            configurable: false,
            writable: false,
        });

        expect(() =>
            installGuardedProperty(
                target,
                'value',
                (getReal: () => () => unknown) =>
                    (...args: unknown[]) =>
                        (getReal() as (...a: unknown[]) => unknown)(...args),
            ),
        ).toThrow(/Cannot redefine property/);
    });

    // A hostile dependency could set globalThis.jest = {} to fake RUNNING_UNDER_JEST outside real
    // Jest. jest.isolateModules can't exercise this — Jest's own module wrapper always injects a
    // real `jest` closure that shadows globalThis.jest — so this spawns a genuine non-Jest process.
    test("Should not treat a bare globalThis.jest override lacking Jest's real shape as running under Jest, in a real non-Jest process", () => {
        const result = child_process
            .execFileSync(
                process.execPath,
                [
                    '-r',
                    'ts-node/register',
                    '-e',
                    // console.log(String(...)), not the bare boolean — a bare boolean can come back
                    // ANSI-colored by Node's own inspect() if the parent's env forces color, corrupting
                    // the exact-match assertion below.
                    'global.jest = {}; const ng = require(process.argv[1]); console.log(String(ng.shouldAllowConfigurableUnderJest(globalThis)));',
                    require.resolve('./network-guard'),
                ],
                {
                    encoding: 'utf8',
                    env: {
                        ...process.env,
                        TS_NODE_TRANSPILE_ONLY: '1',
                        TS_NODE_COMPILER_OPTIONS: '{"module":"commonjs","moduleResolution":"node"}',
                    },
                },
            )
            .trim();

        expect(result).toBe('false');
    });

    // Guarding ChildProcess.prototype.spawn directly (one property, shared by every instance) means
    // a plain `oneChild.spawn = mock` — an ordinary instance-level reassignment, not a hostile
    // bypass — must shadow the guard for that instance only, not repoint the one delegate every
    // other instance's guard still calls through.
    test('Should shadow a guarded property per-instance instead of corrupting the shared delegate when installed on a shared prototype', () => {
        const proto: { value: unknown } = { value: () => 'real' };
        installGuardedProperty(
            proto,
            'value',
            (getReal: () => () => unknown) =>
                (...args: unknown[]) =>
                    (getReal() as (...a: unknown[]) => unknown)(...args),
        );

        const instanceA = Object.create(proto) as { value: unknown };
        const instanceB = Object.create(proto) as { value: unknown };

        instanceA.value = () => 'mocked';

        expect((instanceA.value as () => string)()).toBe('mocked');
        expect((instanceB.value as () => string)()).toBe('real');
        expect((proto.value as () => string)()).toBe('real');
    });

    // Matches a fs write method absent on an older Node runtime: wrapping a method that doesn't
    // exist would make feature-detection lie, then crash the moment a library actually calls it.
    test('Should skip installing a guard entirely when the target property does not exist on this runtime', () => {
        const target: Record<string, unknown> = {};
        installGuardedProperty(target, 'doesNotExist', () => () => 'guard');
        expect(Object.prototype.hasOwnProperty.call(target, 'doesNotExist')).toBe(false);
    });

    // isCurrentlyBlocked() is the shared gate for every guard in this file — a fake AsyncLocalStorage
    // swapped in here (via a plain `fs[symbol] = ...` assignment, which any code holding an `fs`
    // reference could do) would silently disable all of them at once, not just one API surface.
    test('Should protect the AsyncLocalStorage registry entries stashed on `fs` from being overwritten by any code holding an `fs` reference', () => {
        const symbol = Symbol.for('@dd/apps-plugin/network-guard blockedContext');
        const registry = fs as unknown as Record<symbol, unknown>;
        const descriptor = Object.getOwnPropertyDescriptor(registry, symbol);
        expect(descriptor).toMatchObject({ writable: false, configurable: false });

        expect(() => {
            Object.defineProperty(registry, symbol, {
                configurable: true,
                value: { getStore: () => undefined, run: (_v: unknown, fn: () => unknown) => fn() },
            });
        }).toThrow(/Cannot redefine property/);
    });

    // A raw AsyncLocalStorage instance on the registry would let any code with `require('fs')`
    // call `.disable()` on it and permanently kill write blocking process-wide — a stronger
    // bypass than reading a value, since it disarms every future runBlocked call too.
    test('Should not let a `.disable()` call reached via the fs-keyed registry entry disarm write blocking for a later runBlocked call', async () => {
        const symbol = Symbol.for('@dd/apps-plugin/network-guard blockedContext');
        const registry = fs as unknown as Record<symbol, Record<string, unknown>>;
        const entry = registry[symbol];

        expect(typeof entry.isActive).toBe('function');
        expect(typeof entry.run).toBe('function');
        expect(entry.disable).toBeUndefined();
        expect(entry.getStore).toBeUndefined();

        const probePath = path.join(os.tmpdir(), 'dd-network-guard-probe-disable.txt');
        await expect(
            runBlocked(async () => {
                await fs.promises.writeFile(probePath, 'data');
            }),
        ).rejects.toThrow(/Writing to the filesystem is not allowed/);
    });

    // The facade object itself is a plain object; non-writable/non-configurable on the registry
    // property only stops the property from being replaced, not the object's own methods from
    // being reassigned by any code holding an `fs` reference.
    test('Should freeze the shared facade so its isActive/run methods cannot be reassigned', () => {
        const symbol = Symbol.for('@dd/apps-plugin/network-guard blockedContext');
        const registry = fs as unknown as Record<symbol, Record<string, unknown>>;
        const entry = registry[symbol];

        expect(Object.isFrozen(entry)).toBe(true);
        expect(() => {
            entry.isActive = () => false;
        }).toThrow();
    });

    // A lookup that only checks truthiness (`!registry[symbol]`) would treat a value inherited
    // from `fs`'s own prototype chain as already-installed and return it directly, skipping real
    // installation — calling getSharedContext itself against a polluted prototype is what actually
    // exercises that decision, not just re-deriving the inheritance semantics separately. Targets
    // `fs`'s actual prototype rather than assuming it's literally `Object.prototype`, since a
    // sandboxed test runtime can give core modules a different (or null) one.
    test("Should not mistake a value inherited from fs's own prototype chain for an already-installed registry entry", () => {
        const symbol = Symbol.for('@dd/apps-plugin/network-guard pollutionProbe');
        const pollutedFacade = { isActive: () => false, run: (fn: () => unknown) => fn() };
        const fsPrototype = Object.getPrototypeOf(fs) as Record<symbol, unknown>;

        try {
            fsPrototype[symbol] = pollutedFacade;

            const context = getSharedContext('pollutionProbe');

            expect(context).not.toBe(pollutedFacade);
            expect(Object.prototype.hasOwnProperty.call(fs, symbol)).toBe(true);
            expect(context.isActive()).toBe(false);
        } finally {
            // Only the prototype pollution is ours to undo — the real own-property entry
            // getSharedContext just installed is permanent by design, same as every other key.
            delete fsPrototype[symbol];
        }
    });
});

describe('guardWorker', () => {
    // A later reassignment of worker_threads.Worker to undefined (e.g. the same "capture original,
    // mock, restore" idiom exercised elsewhere in this file, with a mock value of undefined) must
    // degrade gracefully instead of crashing installGuardedProperty's setter with `new Proxy(undefined, {})`.
    test('Should return undefined when the real Worker does not exist on this runtime', () => {
        expect(guardWorker(() => undefined)).toBeUndefined();
    });
});

describe('construct-trap newTarget forwarding', () => {
    // Discarding newTarget would make `class Foo extends Worker {}` silently produce a base
    // instance instead — exercised against a fake constructor to avoid real construction side effects.
    test('guardWorker should forward newTarget so a subclass produces an instance of that subclass', () => {
        class FakeWorker {
            options: unknown;
            constructor(options: unknown) {
                this.options = options;
            }
        }
        const Guarded = guardWorker(
            () => FakeWorker as unknown as typeof worker_threads.Worker,
        ) as unknown as new (options: unknown) => object;
        class CustomWorker extends Guarded {}

        const instance = new CustomWorker({});
        expect(instance).toBeInstanceOf(CustomWorker);
    });
});

describe('trustedFetch', () => {
    // Regression test: a customer function can reassign globalThis.fetch to an attacker-controlled
    // wrapper (e.g. to capture the dev server's authenticated request while inside runAllowed).
    // trustedFetch is captured once at module load, before any customer code can run, so it must
    // keep resolving to the real implementation regardless of later reassignment — independent of
    // whether customer-initiated fetch calls are themselves blocked (they aren't, by design).
    test('Should stay immune to globalThis.fetch being reassigned after module load', () => {
        const attackerFetch = jest.fn().mockResolvedValue(new Response('stolen'));
        const originalFetch = globalThis.fetch;
        setGlobalFetch(attackerFetch as unknown as typeof fetch);

        try {
            expect(trustedFetch).not.toBe(attackerFetch);
            expect(trustedFetch).not.toBe(globalThis.fetch);
        } finally {
            setGlobalFetch(originalFetch);
        }
    });

    test('Should remain unaffected by runBlocked', async () => {
        const before = trustedFetch;
        await runBlocked(async () => {
            expect(trustedFetch).toBe(before);
        });
        expect(trustedFetch).toBe(before);
    });
});
