import stream from "node:stream";

// ReadableLimiter is a class that limits the amount of
// data to read. It will never return more data than the configured limit.
// However, it doesn't guarantee that it reads less than the limit from the passed reader.
export class ReadableLimiter extends stream.Readable {
  public written: number = 0;
  private leftover: Uint8Array | undefined;

  constructor(
    // reader will be used to read bytes until limit.
    // it might read more than 'limit' due to Bun not supporting byob.
    // We workaround this by keeping track of the previousReader that the caller should pass.
    private reader: ReadableStreamDefaultReader<Uint8Array>,
    private limit: number,
    previousReader?: ReadableLimiter,
  ) {
    super();

    if (previousReader) this.leftover = previousReader.leftover;
  }

  _read(): void {
    if (this.limit === 0) {
      this.push(null);
    }

    if (this.leftover !== undefined) {
      const toPushNow = this.leftover.slice(0, this.limit);
      this.leftover = this.leftover.slice(this.limit);
      this.push(toPushNow);
      this.limit -= toPushNow.length;
      this.written += toPushNow.length;

      // if no leftovers left to write from before
      if (this.leftover.length == 0) {
        this.leftover = undefined;
      }
      return;
    }

    this.reader.read().then((result) => {
      if (result.done) return this.push(null);

      let arr = result.value as Uint8Array;
      if (arr.length > this.limit) {
        const toPushNow = arr.slice(0, this.limit);
        this.leftover = arr.slice(this.limit);
        arr = toPushNow;
      }

      if (arr.length === 0) return this.push(null);

      this.push(arr);
      this.limit -= arr.length;
      this.written += arr.length;
    });
  }
}