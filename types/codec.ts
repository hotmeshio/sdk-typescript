/**
 * Synchronous codec for encoding/decoding serialized payload data.
 *
 * Register a `PayloadCodec` to transparently transform all serialized
 * object values (`/s{json}`) before they are stored and after they are
 * read. Common use cases: encryption at rest, compression, or custom
 * binary encoding.
 *
 * ## How It Works
 *
 * The serializer stores JavaScript objects as `/s{json}`. When a codec
 * is registered, objects are instead stored as `/b{encoded}`:
 *
 * ```
 * Write: value → JSON.stringify → codec.encode(json) → "/b{encoded}"
 * Read:  "/b{encoded}" → codec.decode(encoded) → JSON.parse → value
 *        "/s{json}" → JSON.parse → value  (backward compatible)
 * ```
 *
 * ## Constraints
 *
 * - **Synchronous** — `encode` and `decode` must be synchronous.
 *   Use Node.js `crypto` (e.g., `createCipheriv`) or `zlib`
 *   (e.g., `deflateSync`) for sync operations.
 * - **String-safe** — `encode` output must be a valid UTF-8 string
 *   (Postgres TEXT column). Use base64 encoding for binary output.
 * - **Deterministic decode** — `decode(encode(x))` must equal `x`.
 *
 * ## Scope
 *
 * The codec applies to all data flowing through the serializer's
 * `package`/`unpackage` path — job state, activity inputs/outputs,
 * timeline markers, and workflow metadata. It does **not** apply to
 * `search()`, `entity()`, or `enrich()` data, which bypass the
 * serializer.
 *
 * ## Example: AES-256-GCM Encryption
 *
 * ```typescript
 * import { createCipheriv, createDecipheriv, randomBytes } from 'crypto';
 * import { PayloadCodec } from '@hotmeshio/hotmesh';
 *
 * const key = randomBytes(32);
 *
 * const codec: PayloadCodec = {
 *   encode(json: string): string {
 *     const iv = randomBytes(12);
 *     const cipher = createCipheriv('aes-256-gcm', key, iv);
 *     const enc = Buffer.concat([cipher.update(json, 'utf8'), cipher.final()]);
 *     const tag = cipher.getAuthTag();
 *     return Buffer.concat([iv, tag, enc]).toString('base64');
 *   },
 *   decode(encoded: string): string {
 *     const buf = Buffer.from(encoded, 'base64');
 *     const iv = buf.subarray(0, 12);
 *     const tag = buf.subarray(12, 28);
 *     const data = buf.subarray(28);
 *     const decipher = createDecipheriv('aes-256-gcm', key, iv);
 *     decipher.setAuthTag(tag);
 *     return Buffer.concat([decipher.update(data), decipher.final()]).toString('utf8');
 *   },
 * };
 *
 * // Register globally — applies to all HotMesh and Durable instances
 * HotMesh.registerCodec(codec);
 * ```
 */
export interface PayloadCodec {
  /**
   * Encode a JSON string before storage. The output replaces
   * the `/s{json}` prefix with `/b{encoded}`.
   *
   * @param json - The raw JSON string (result of JSON.stringify)
   * @returns The encoded string (must be valid UTF-8)
   */
  encode(json: string): string;

  /**
   * Decode a previously encoded string back to the original JSON.
   *
   * @param encoded - The encoded string (from a previous `encode` call)
   * @returns The original JSON string
   */
  decode(encoded: string): string;
}
