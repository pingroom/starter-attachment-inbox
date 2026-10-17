export function shutdownSignal() {
  const controller = new AbortController();
  let watchdog;
  const stop = (name) => {
    if (controller.signal.aborted) return;
    process.exitCode = name === 'SIGINT' ? 130 : 143;
    controller.abort(new Error(`Stopped by ${name}.`));
    watchdog = setTimeout(() => process.exit(process.exitCode), 10_000);
    watchdog.unref();
  };
  const interrupt = () => stop('SIGINT');
  const terminate = () => stop('SIGTERM');
  process.on('SIGINT', interrupt);
  process.on('SIGTERM', terminate);
  return { signal: controller.signal, dispose() {
    process.off('SIGINT', interrupt);
    process.off('SIGTERM', terminate);
    if (!controller.signal.aborted) clearTimeout(watchdog);
  } };
}
