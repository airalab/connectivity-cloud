/**
 * Copyright 2026 Robonomics Network
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { PRUNE_STALE_SENSOR_SCRIPT } from '../src/index.js';

function parseBound(bound: number | string): {
  value: number;
  exclusive: boolean;
} {
  if (typeof bound === 'number') return { value: bound, exclusive: false };
  if (bound === '-inf') return { value: -Infinity, exclusive: false };
  if (bound === '+inf') return { value: Infinity, exclusive: false };
  return bound.startsWith('(')
    ? { value: Number(bound.slice(1)), exclusive: true }
    : { value: Number(bound), exclusive: false };
}

/** In-memory Redis double that mirrors the commands used by the heartbeat tracker. */
export class FakeRedis {
  /** Number of Redis commands issued, by command name. */
  readonly commands: Record<string, number> = {};
  /** Total number of hash/set/sorted-set members returned to the caller. */
  membersRead = 0;
  private readonly zsets = new Map<string, Map<string, number>>();
  private readonly hashes = new Map<string, Record<string, string>>();
  private readonly sets = new Map<string, Set<string>>();

  private count(command: string): void {
    this.commands[command] = (this.commands[command] ?? 0) + 1;
  }

  private inRange(
    key: string,
    min: number | string,
    max: number | string
  ): Array<[string, number]> {
    const low = parseBound(min);
    const high = parseBound(max);
    return [...(this.zsets.get(key) ?? new Map<string, number>())].filter(
      ([, score]) =>
        (low.exclusive ? score > low.value : score >= low.value) &&
        (high.exclusive ? score < high.value : score <= high.value)
    );
  }

  async hgetall(key: string): Promise<Record<string, string>> {
    this.count('hgetall');
    this.membersRead += 1;
    return { ...(this.hashes.get(key) ?? {}) };
  }

  async hset(key: string, map: Record<string, string>): Promise<number> {
    const current = this.hashes.get(key) ?? {};
    this.hashes.set(key, { ...current, ...map });
    return Object.keys(map).length;
  }

  async sadd(key: string, member: string): Promise<number> {
    const set = this.sets.get(key) ?? new Set<string>();
    const before = set.size;
    set.add(member);
    this.sets.set(key, set);
    return set.size > before ? 1 : 0;
  }

  async smembers(key: string): Promise<string[]> {
    this.count('smembers');
    const members = [...(this.sets.get(key) ?? new Set<string>())];
    this.membersRead += members.length;
    return members;
  }

  async zcount(
    key: string,
    min: number | string,
    max: number | string
  ): Promise<number> {
    this.count('zcount');
    return this.inRange(key, min, max).length;
  }

  async zrevrangebyscore(
    key: string,
    max: number | string,
    min: number | string,
    _limitToken: 'LIMIT',
    offset: number,
    count: number
  ): Promise<string[]> {
    this.count('zrevrangebyscore');
    const members = this.inRange(key, min, max)
      .sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? 1 : -1))
      .slice(offset, offset + count)
      .map(([member]) => member);
    this.membersRead += members.length;
    return members;
  }

  async zrangebyscore(
    key: string,
    min: number | string,
    max: number | string,
    _limitToken: 'LIMIT',
    offset: number,
    count: number
  ): Promise<string[]> {
    this.count('zrangebyscore');
    const members = this.inRange(key, min, max)
      .sort((a, b) => a[1] - b[1] || (a[0] < b[0] ? -1 : 1))
      .slice(offset, offset + count)
      .map(([member]) => member);
    this.membersRead += members.length;
    return members;
  }

  async zrem(key: string, ...members: string[]): Promise<number> {
    const zset = this.zsets.get(key);
    return zset ? members.filter((member) => zset.delete(member)).length : 0;
  }

  async srem(key: string, member: string): Promise<number> {
    const set = this.sets.get(key);
    if (!set) return 0;
    const deleted = set.delete(member) ? 1 : 0;
    if (set.size === 0) {
      this.sets.delete(key);
    }
    return deleted;
  }

  async del(key: string): Promise<number> {
    const existed = this.hashes.has(key) ? 1 : 0;
    this.hashes.delete(key);
    return existed;
  }

  /**
   * Test double for the Lua scripts used in production. The heartbeat script
   * only advances `lastSeen` when the incoming event timestamp is strictly
   * newer than what is stored; the prune script only removes a sensor that is
   * still stale at execution time.
   */
  async eval(
    script: string,
    numkeys: number,
    ...rest: Array<string | number>
  ): Promise<unknown> {
    if (script === PRUNE_STALE_SENSOR_SCRIPT) {
      return this.evalPrune(numkeys, rest);
    }
    return this.evalHeartbeat(numkeys, rest);
  }

  private async evalPrune(
    numkeys: number,
    rest: Array<string | number>
  ): Promise<number> {
    const [key, sensorsKey, lastSeenIndexKey] = rest.slice(
      0,
      numkeys
    ) as string[];
    const [sensorId, cutoffRaw] = rest.slice(numkeys);
    if (
      key === undefined ||
      sensorsKey === undefined ||
      lastSeenIndexKey === undefined ||
      typeof sensorId !== 'string'
    ) {
      throw new Error('missing arguments for prune eval');
    }
    const hashLastSeen = Number.parseInt(
      this.hashes.get(key)?.lastSeen ?? '',
      10
    );
    const indexScore = this.zsets.get(lastSeenIndexKey)?.get(sensorId);
    const lastSeen = Number.isFinite(hashLastSeen) ? hashLastSeen : indexScore;
    if (lastSeen !== undefined && lastSeen >= Number(cutoffRaw)) return 0;
    await this.srem(sensorsKey, sensorId);
    await this.del(key);
    await this.zrem(lastSeenIndexKey, sensorId);
    return 1;
  }

  private async evalHeartbeat(
    numkeys: number,
    rest: Array<string | number>
  ): Promise<[number, number, number, number, number]> {
    const keys = rest.slice(0, numkeys) as string[];
    const args = rest.slice(numkeys);
    const [key, sensorsKey, lastSeenIndexKey] = keys;
    const [sensorId, eventTimeRaw, onlineWindowMsRaw] = args;
    const eventTime = Number(eventTimeRaw);
    const onlineWindowMs = Number(onlineWindowMsRaw);

    if (
      key === undefined ||
      sensorsKey === undefined ||
      lastSeenIndexKey === undefined
    ) {
      throw new Error('missing keys for eval');
    }
    if (typeof sensorId !== 'string') {
      throw new Error('missing sensorId arg for eval');
    }

    await this.sadd(sensorsKey, sensorId);

    const existing = this.hashes.get(key) ?? {};
    const lastSeen = Number.parseInt(existing.lastSeen ?? '', 10);
    const firstSeen = Number.parseInt(existing.firstSeen ?? '', 10);
    const onlineSince = Number.parseInt(existing.onlineSince ?? '', 10);
    const hasLastSeen = Number.isFinite(lastSeen);
    const hasFirstSeen = Number.isFinite(firstSeen);
    const hasOnlineSince = Number.isFinite(onlineSince);
    const isNew = hasFirstSeen ? 0 : 1;

    if (!hasLastSeen || eventTime > lastSeen) {
      const newFirstSeen = hasFirstSeen ? firstSeen : eventTime;
      let newOnlineSince = hasOnlineSince ? onlineSince : eventTime;
      if (hasLastSeen && eventTime - lastSeen > onlineWindowMs) {
        newOnlineSince = eventTime;
      }
      this.hashes.set(key, {
        firstSeen: String(newFirstSeen),
        lastSeen: String(eventTime),
        onlineSince: String(newOnlineSince),
      });
      const index = this.zsets.get(lastSeenIndexKey) ?? new Map();
      index.set(sensorId, eventTime);
      this.zsets.set(lastSeenIndexKey, index);
      return [1, isNew, newFirstSeen, eventTime, newOnlineSince];
    }

    return [
      0,
      isNew,
      hasFirstSeen ? firstSeen : eventTime,
      lastSeen,
      hasOnlineSince ? onlineSince : eventTime,
    ];
  }
}
