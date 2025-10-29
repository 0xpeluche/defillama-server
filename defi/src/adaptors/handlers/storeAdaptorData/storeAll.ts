import '../../../api2/utils/failOnError';

import { elastic } from '@defillama/sdk';
import { handler2 } from ".";
import { getUnixTimeNow } from '../../../api2/utils/time';
import { deadChains } from '../../../storeTvlInterval/getAndStoreTvl';
import { getTimestampAtStartOfDayUTC } from '../../../utils/date';
import loadAdaptorsData from '../../data';
import { ADAPTER_TYPES, AdapterType } from "../../data/types";
import { getAllDimensionsRecordsOnDate } from '../../db-utils/db2';

const onlyYesterday = process.env.ONLY_YESTERDAY === 'true';

let maxConcurrency = 21;
if (process.env.DIM_RUN_MAX_CONCURRENCY) {
  const parsed = parseInt(process.env.DIM_RUN_MAX_CONCURRENCY);
  if (!isNaN(parsed)) maxConcurrency = parsed;
}

const adapterTypeStaggerDelay = +(process.env.ADAPTER_TYPE_STAGGER_DELAY_MS ?? 2000);
const adapterTypeJitterMs = +(process.env.ADAPTER_TYPE_JITTER_MS ?? 1500);
const startupJitterMs = +(process.env.STARTUP_JITTER_MS ?? 60000);

// category concurrency (CATEGORY_CONC_MAX has priority; fallback to legacy CATEGORY_CONCURRENCY)
const categoryConcMax = +(process.env.CATEGORY_CONC_MAX ?? process.env.CATEGORY_CONCURRENCY ?? 10);
let   categoryConcStart = +(process.env.CATEGORY_CONC_START ?? Math.min(4, categoryConcMax));
const categoryConcRampMs = +(process.env.CATEGORY_CONC_RAMP_MS ?? 120000);
if (categoryConcStart > categoryConcMax) categoryConcStart = categoryConcMax;

// timer config
const MAX_RUNTIME = 1000 * 60 * +(process.env.MAX_RUNTIME_MINUTES ?? 50);
const MAX_RUNTIME_HARD_CAP_MINUTES = process.env.MAX_RUNTIME_HARD_CAP_MINUTES ? +process.env.MAX_RUNTIME_HARD_CAP_MINUTES : Number.POSITIVE_INFINITY;
const USE_TIMEOUT_AUTO_BUFFER = (process.env.USE_TIMEOUT_AUTO_BUFFER ?? 'true') === 'true';
const TIMEOUT_BUFFER_MS_ENV = process.env.TIMEOUT_BUFFER_MS ? +process.env.TIMEOUT_BUFFER_MS : null;
const SOFT_STOP_MS = +(process.env.SOFT_STOP_MS ?? 90_000);

function estimateDelayBufferMs(numCategories: number) {
  const b1 = numCategories * adapterTypeStaggerDelay + adapterTypeJitterMs + startupJitterMs;
  const b2 = (numCategories / Math.max(1, categoryConcStart)) * adapterTypeStaggerDelay + adapterTypeJitterMs + startupJitterMs;
  return Math.min(b1, b2);
}
const numCategories = ADAPTER_TYPES.length;
const AUTO_BUFFER_MS = USE_TIMEOUT_AUTO_BUFFER ? estimateDelayBufferMs(numCategories) : 0;
const EFFECTIVE_BUFFER_MS = TIMEOUT_BUFFER_MS_ENV ?? AUTO_BUFFER_MS;
const adjustedMaxRuntime = Math.min(MAX_RUNTIME + EFFECTIVE_BUFFER_MS, MAX_RUNTIME_HARD_CAP_MINUTES * 60_000);

function sleep(ms: number) { return new Promise(res => setTimeout(res, ms)); }

async function run() {
  // minimal startup config logs
  console.log('[config] runtimeMin=%s bufferMs=%s hardCapMin=%s adjustedMs=%s',
    (MAX_RUNTIME/60000).toFixed(2),
    EFFECTIVE_BUFFER_MS,
    Number.isFinite(MAX_RUNTIME_HARD_CAP_MINUTES) ? MAX_RUNTIME_HARD_CAP_MINUTES : 'none',
    adjustedMaxRuntime
  );
  console.log('[config] catConc %s -> %s (ramp %sms) | jitterStart=%sms | typeStagger=%sms±%sms',
    categoryConcStart, categoryConcMax, categoryConcRampMs,
    startupJitterMs,
    adapterTypeStaggerDelay, adapterTypeJitterMs
  );

  const startTimeAllUnix = getUnixTimeNow();
  const startTimeAllMs = Date.now();
  console.time("**** Run All Adaptor types");

  if (startupJitterMs > 0) {
    const startupDelay = Math.floor(Math.random() * startupJitterMs);
    await sleep(startupDelay);
  }

  let currentCategoryCap = categoryConcStart;
  setTimeout(() => {
    currentCategoryCap = categoryConcMax;
    console.log(`[ramp] category concurrency -> ${categoryConcMax}`);
  }, categoryConcRampMs);

  const randomizedAdapterTypes = [...ADAPTER_TYPES].sort(() => Math.random() - 0.5);

  let running = 0;
  let launched = 0;
  const queue: Array<{ promise: Promise<void>; done: boolean }> = [];

  for (let i = 0; i < randomizedAdapterTypes.length; i++) {
    const adapterType = randomizedAdapterTypes[i];

    const elapsedMs = Date.now() - startTimeAllMs;
    if (elapsedMs > adjustedMaxRuntime - SOFT_STOP_MS) {
      const remaining = randomizedAdapterTypes.length - i;
      const left = Math.max(0, Math.floor((adjustedMaxRuntime - elapsedMs)/1000));
      console.warn(`[soft-stop] ${left}s remaining, skipping ${remaining} categories`);
      break;
    }

    while (running >= currentCategoryCap) {
      const inflight = queue.filter(item => !item.done).map(item => item.promise);
      if (inflight.length === 0) break;
      await Promise.race(inflight);
      for (let k = queue.length - 1; k >= 0; k--) if (queue[k].done) queue.splice(k, 1);
    }

    const baseDelay = launched * adapterTypeStaggerDelay;
    const jitter = Math.floor((Math.random() * 2 - 1) * adapterTypeJitterMs);
    const delay = Math.max(0, baseDelay + jitter);
    launched += 1;

    const queueItem: { promise: Promise<void>; done: boolean } = { promise: Promise.resolve(), done: false };

    queueItem.promise = (async () => {
      try {
        running++;
        if (delay > 0) await sleep(delay);
        await runAdapterType(adapterType);
      } finally {
        running--;
        queueItem.done = true;
      }
    })();

    queue.push(queueItem);
  }

  await Promise.allSettled(queue.map(item => item.promise));

  console.timeEnd("**** Run All Adaptor types");
  const endTimeAll = getUnixTimeNow();
  await elastic.addRuntimeLog({
    runtime: endTimeAll - startTimeAllUnix,
    success: true,
    metadata: { application: "dimensions", type: 'all', name: 'all' }
  });

  async function runAdapterType(adapterType: AdapterType) {
    const startTimeCategory = getUnixTimeNow()
    // if (adapterType !== AdapterType.AGGREGATORS) return;
    const key = "**** Run Adaptor type: " + adapterType
    console.time(key)
    let success = false

    try {

      let yesterdayIdSet: Set<string> = new Set()
      let todayIdSet: Set<string> = new Set()
      let yesterdayDataMap: Map<string, any> = new Map()
      let todayDataMap: Map<string, any> = new Map()

      try {
        const yesterdayData = await getAllDimensionsRecordsOnDate({ adapterType, date: getYesterdayTimeS() });
        const todayData = await getAllDimensionsRecordsOnDate({ adapterType, date: getTodayTimeS() });

        // Create maps
        yesterdayDataMap = new Map(yesterdayData.map((d: any) => [d.id, d]));
        todayDataMap = new Map(todayData.map((d: any) => [d.id, d]));
        todayIdSet = new Set(todayData.map((d: any) => d.id));

        // Load adaptor data to check dependencies and versions
        const dataModule = loadAdaptorsData(adapterType);
        const { protocolAdaptors, importModule } = dataModule;

        // Smart filtering: Build yesterdayIdSet by checking each protocol (similar to handler2 logic)
        const now = new Date();
        const currentHour = now.getUTCHours();
        const startOfTodayTimestamp = getTimestampAtStartOfDayUTC(Math.floor(Date.now() / 1000)); // 00:00 UTC today
        const isAfter1AM = currentHour >= 1;
        const isAfter8AM = currentHour >= 8;

        // Process each protocol to determine if it should be in yesterdayIdSet
        for (const protocol of protocolAdaptors) {
          const id2 = protocol.id2;
          const yesterdayRecord = yesterdayDataMap.get(id2);
          // If no yesterday data exists, don't add to set (will trigger refill)
          if (!yesterdayRecord) continue;

          let includeInSet = true;

          try {
            const adaptor = await importModule(protocol.module);
            const version = adaptor.version ?? 1;
            const runAtCurrTime = adaptor.runAtCurrTime ?? false;
            const hasDuneDependency = adaptor.dependencies?.includes('dune' as any) ?? false;
            const isV1 = version === 1;
            const isV2 = !isV1;
            // Edge case 1: V2 adapters (not runAtCurrTime) with incomplete yesterday data
            // Only applies to V2 adapters that don't run at current time
            // Remove from set to trigger refill for incomplete data after 01:00 UTC
            if (isV2 && !runAtCurrTime && yesterdayRecord.updatedAt && yesterdayRecord.updatedAt < startOfTodayTimestamp && isAfter1AM) {
              includeInSet = false;
              console.log(`Removing ${id2} from yesterdayIdSet - incomplete V2 data (last update: ${new Date(yesterdayRecord.updatedAt * 1000).toISOString()})`)
            }

            // Edge case 2: V1 DUNE adapters with data updated before 08:00 UTC
            // Only applies to V1 adapters with DUNE dependencies
            // Remove from set to trigger refill after 08:00 UTC
            if (includeInSet && isV1 && hasDuneDependency && yesterdayRecord.updatedAt && isAfter8AM) {
              const lastUpdateDate = new Date(yesterdayRecord.updatedAt * 1000);
              const lastUpdateHourUTC = lastUpdateDate.getUTCHours();
              if (lastUpdateHourUTC < 8) {
                includeInSet = false;
                console.log(`Removing ${id2} from yesterdayIdSet - V1 DUNE adapter needs refresh (last update: ${lastUpdateDate.toISOString()}, before 08:00 UTC)`)
              }
            }
          } catch (e) {
            console.error(`importModule error for ${id2} - ${protocol.module}: ${e}`)
            includeInSet = true;
          }

          if (includeInSet) yesterdayIdSet.add(id2);
        }
      } catch (e) {
        console.error("Error in getAllDimensionsRecordsOnDate", e)
      }

      await handler2({
        adapterType,
        yesterdayIdSet,
        runType: 'store-all',
        todayIdSet,
        maxRunTime: MAX_RUNTIME - 2 * 60 * 1000,
        onlyYesterday,
        maxConcurrency,
        deadChains,
      });

      success = true;

    } catch (e) {
      console.error("error", e)
      await elastic.addErrorLog({
        error: e as any,
        metadata: {
          application: "dimensions",
          type: 'category',
          name: adapterType,
        }
      })
    }

    console.timeEnd(key);
    const endTimeCategory = getUnixTimeNow();
    await elastic.addRuntimeLog({
      runtime: endTimeCategory - startTimeCategory,
      success,
      metadata: {
        application: "dimensions",
        isCategory: true,
        category: adapterType,
      }
    })
  }
}

setTimeout(() => {
  console.error("Timeout reached, exiting from dimensions-store-all...")
  process.exit(1)
}, adjustedMaxRuntime)

run().catch((e) => {
  console.error("Error in dimensions-store-all", e)
}).then(() => process.exit(0))



function getYesterdayTimeS() {
  const yesterday = new Date();
  yesterday.setUTCDate(yesterday.getUTCDate() - 1);
  const yyyy = yesterday.getUTCFullYear();
  const mm = String(yesterday.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(yesterday.getUTCDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

function getTodayTimeS() {
  const today = new Date();
  const yyyy = today.getUTCFullYear();
  const mm = String(today.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(today.getUTCDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}