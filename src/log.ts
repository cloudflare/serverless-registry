type LogFields = Record<string, string | number | boolean | null | undefined>;

function emit(level: "info" | "warn" | "error", event: string, fields?: LogFields): void {
  const line = JSON.stringify({ level, event, ts: new Date().toISOString(), ...fields });
  if (level === "error") {
    console.error(line);
  } else if (level === "warn") {
    console.warn(line);
  } else {
    console.log(line);
  }
}

export const log = {
  info: (event: string, fields?: LogFields): void => emit("info", event, fields),
  warn: (event: string, fields?: LogFields): void => emit("warn", event, fields),
  error: (event: string, fields?: LogFields): void => emit("error", event, fields),
};
