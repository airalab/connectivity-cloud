import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

/** Write `content` to a fresh temp file and return its path. */
export function writeSeedFile(content: string): string {
  const path = join(mkdtempSync(join(tmpdir(), 'anchor-seed-')), 'suri');
  writeFileSync(path, content);
  return path;
}
