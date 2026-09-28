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

/**
 * Default maximum size, in bytes, of a chain-ready payload passed to
 * `CPS.set_payload`. This is the single source of truth for the 8 KiB CPS
 * payload limit; services may override it via environment configuration but
 * should fall back to this default rather than hard-coding the literal
 * value themselves.
 */
export const DEFAULT_ANCHOR_MAX_PAYLOAD_BYTES = 8192;
