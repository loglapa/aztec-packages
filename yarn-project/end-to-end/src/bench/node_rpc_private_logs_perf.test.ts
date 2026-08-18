/**
 * Focused benchmark for `getPrivateLogsByTags`, comparing the unrepresentative single-random-tag in-process
 * call against the production shape: 100 tags per call, real tags that actually match logs, a referenceBlock
 * anchor, and going over HTTP JSON-RPC. Also probes serialization behind the lmdb writer queue.
 */
import { AztecAddress } from '@aztec/aztec.js/addresses';
import type { Logger } from '@aztec/aztec.js/log';
import type { AztecNode } from '@aztec/aztec.js/node';
import { BlockNumber } from '@aztec/foundation/branded-types';
import { createNamespacedSafeJsonRpcServer, startHttpRpcServer } from '@aztec/foundation/json-rpc/server';
import { Timer } from '@aztec/foundation/timer';
import { TokenContract } from '@aztec/noir-contracts.js/Token';
import type { BlockHash } from '@aztec/stdlib/block';
import { AztecNodeApiSchema, createAztecNodeClient } from '@aztec/stdlib/interfaces/client';
import type { AztecNodeDebug } from '@aztec/stdlib/interfaces/client';
import { SiloedTag } from '@aztec/stdlib/logs';
import type { TxHash } from '@aztec/stdlib/tx';

import { jest } from '@jest/globals';
import type { Server } from 'http';
import 'jest-extended';

import { PIPELINING_SETUP_OPTS } from '../fixtures/fixtures.js';
import { setup } from '../fixtures/utils.js';
import type { TestWallet } from '../test-wallet/test_wallet.js';
import { proveInteraction } from '../test-wallet/utils.js';

const BENCHMARK_ITERATIONS = 20;
const BLOCKS_TO_BUILD = 5;
const TAGS_PER_CALL = 100;

interface TimingStats {
  avg: number;
  min: number;
  max: number;
  total: number;
  count: number;
}

function calculateStats(timings: number[]): TimingStats {
  if (timings.length === 0) {
    return { avg: 0, min: 0, max: 0, total: 0, count: 0 };
  }
  const total = timings.reduce((a, b) => a + b, 0);
  return {
    avg: total / timings.length,
    min: Math.min(...timings),
    max: Math.max(...timings),
    total,
    count: timings.length,
  };
}

async function benchmark<T>(fn: () => Promise<T>, iterations: number = BENCHMARK_ITERATIONS): Promise<TimingStats> {
  const timings: number[] = [];
  for (let i = 0; i < iterations; i++) {
    const timer = new Timer();
    await fn();
    timings.push(timer.ms());
  }
  return calculateStats(timings);
}

describe('e2e_node_rpc_private_logs_perf', () => {
  jest.setTimeout(20 * 60 * 1000);

  let logger: Logger;
  let aztecNode: AztecNode & AztecNodeDebug;
  let httpNode: AztecNode;
  let httpServer: Server & { port: number };
  let wallet: TestWallet;
  let ownerAddress: AztecAddress;
  let teardown: () => Promise<void>;

  let tokenContract: TokenContract;
  let referenceBlock: BlockHash;
  let realTags: SiloedTag[] = [];
  const txHashes: TxHash[] = [];

  const results: { name: string; stats: TimingStats }[] = [];

  function record(name: string, stats: TimingStats) {
    results.push({ name, stats });
    const line = `RESULT ${name}: avg=${stats.avg.toFixed(2)}ms min=${stats.min.toFixed(2)}ms max=${stats.max.toFixed(
      2,
    )}ms`;
    logger.info(line);
    // eslint-disable-next-line no-console
    console.log(line);
  }

  function tagsOfSize(size: number, source: SiloedTag[]): SiloedTag[] {
    const out: SiloedTag[] = [];
    for (let i = 0; i < size; i++) {
      out.push(source.length > 0 ? source[i % source.length] : SiloedTag.random());
    }
    return out;
  }

  afterAll(async () => {
    logger.info('=== getPrivateLogsByTags benchmark summary ===');
    // eslint-disable-next-line no-console
    console.log('=== getPrivateLogsByTags benchmark summary ===');
    for (const { name, stats } of results) {
      const line = `RESULT ${name}: avg=${stats.avg.toFixed(2)}ms min=${stats.min.toFixed(
        2,
      )}ms max=${stats.max.toFixed(2)}ms`;
      logger.info(line);
      // eslint-disable-next-line no-console
      console.log(line);
    }
    httpServer?.close();
    await teardown();
  });

  beforeAll(async () => {
    ({
      teardown,
      logger,
      aztecNode,
      wallet,
      accounts: [ownerAddress],
    } = await setup(1, {
      archiverPollingIntervalMS: 200,
      sequencerPollingIntervalMS: 200,
      worldStateBlockCheckIntervalMS: 200,
      blockCheckIntervalMS: 200,
      ...PIPELINING_SETUP_OPTS,
      minTxsPerBlock: 1,
    }));

    logger.info('Deploying token contract...');
    ({ contract: tokenContract } = await TokenContract.deploy(wallet, ownerAddress, 'TestToken', 'TST', 18n).send({
      from: ownerAddress,
    }));
    logger.info(`Token contract deployed at ${tokenContract.address}`);

    const firstBlock = await aztecNode.getBlockNumber();

    logger.info(`Building ${BLOCKS_TO_BUILD} blocks with private minting txs...`);
    for (let i = 0; i < BLOCKS_TO_BUILD; i++) {
      const provenTx = await proveInteraction(wallet, tokenContract.methods.mint_to_private(ownerAddress, 100n), {
        from: ownerAddress,
      });
      const receipt = await provenTx.send({ wait: { timeout: 600 } });
      txHashes.push(receipt.txHash);
      logger.info(`Block ${i + 1}/${BLOCKS_TO_BUILD} built (tx ${receipt.txHash} in block ${receipt.blockNumber})`);
    }

    const lastBlock = await aztecNode.getBlockNumber();

    // Harvest real siloed tags: the first field of every private log in every tx effect of every block.
    const blocks = await aztecNode.getBlocks(BlockNumber(1), lastBlock, { includeTransactions: true });
    const harvested: SiloedTag[] = [];
    for (const block of blocks) {
      for (const txEffect of block.body.txEffects) {
        for (const log of txEffect.privateLogs) {
          harvested.push(new SiloedTag(log.fields[0]));
        }
      }
    }
    realTags = harvested;
    const harvestLine = `Harvested ${realTags.length} real siloed tags from ${blocks.length} blocks (1..${lastBlock}, first tx in block ${firstBlock})`;
    logger.info(harvestLine);
    // eslint-disable-next-line no-console
    console.log(harvestLine);

    referenceBlock = blocks[blocks.length - 1].hash;

    // Wrap the in-process node in an HTTP JSON-RPC server so we can measure serialization + transport cost.
    const rpcServer = createNamespacedSafeJsonRpcServer(
      { aztec: [aztecNode, AztecNodeApiSchema] },
      { maxBodySizeBytes: '50mb' },
    );
    httpServer = await startHttpRpcServer(rpcServer, { host: '127.0.0.1', port: 0 });
    httpNode = createAztecNodeClient(`http://127.0.0.1:${httpServer.port}`);
    logger.info(`Node JSON-RPC server listening on port ${httpServer.port}`);

    // Sanity check that the HTTP path works and that the real tags actually match logs.
    const sample = await httpNode.getPrivateLogsByTags({ tags: tagsOfSize(TAGS_PER_CALL, realTags) });
    const matched = sample.reduce((acc, arr) => acc + arr.length, 0);
    const sanityLine = `Sanity: HTTP call with ${TAGS_PER_CALL} real tags matched ${matched} logs`;
    logger.info(sanityLine);
    // eslint-disable-next-line no-console
    console.log(sanityLine);
  });

  it('benchmarks the full getPrivateLogsByTags matrix', async () => {
    expect(realTags.length).toBeGreaterThan(0);

    record(
      'inproc_1_random_tag',
      await benchmark(() => aztecNode.getPrivateLogsByTags({ tags: [SiloedTag.random()] })),
    );

    record(
      'inproc_100_random_tags',
      await benchmark(() => aztecNode.getPrivateLogsByTags({ tags: tagsOfSize(TAGS_PER_CALL, []) })),
    );

    record(
      'inproc_100_real_tags',
      await benchmark(() => aztecNode.getPrivateLogsByTags({ tags: tagsOfSize(TAGS_PER_CALL, realTags) })),
    );

    record(
      'inproc_100_real_tags_refblock',
      await benchmark(() =>
        aztecNode.getPrivateLogsByTags({ tags: tagsOfSize(TAGS_PER_CALL, realTags), referenceBlock }),
      ),
    );

    record('http_1_random_tag', await benchmark(() => httpNode.getPrivateLogsByTags({ tags: [SiloedTag.random()] })));

    record(
      'http_100_random_tags',
      await benchmark(() => httpNode.getPrivateLogsByTags({ tags: tagsOfSize(TAGS_PER_CALL, []) })),
    );

    record(
      'http_100_real_tags',
      await benchmark(() => httpNode.getPrivateLogsByTags({ tags: tagsOfSize(TAGS_PER_CALL, realTags) })),
    );

    record(
      'http_100_real_tags_refblock',
      await benchmark(() =>
        httpNode.getPrivateLogsByTags({ tags: tagsOfSize(TAGS_PER_CALL, realTags), referenceBlock }),
      ),
    );

    record('http_getBlockNumber', await benchmark(() => httpNode.getBlockNumber()));

    record('http_getTxReceipt', await benchmark(() => httpNode.getTxReceipt(txHashes[0])));

    // Contention probe: issue 5 concurrent calls without awaiting them, then time one more call issued
    // immediately after. If the store serializes reads, the measured latency grows with the queue depth.
    const queuedTimings: number[] = [];
    for (let i = 0; i < BENCHMARK_ITERATIONS; i++) {
      const inFlight = Array.from({ length: 5 }, () =>
        aztecNode.getPrivateLogsByTags({ tags: tagsOfSize(TAGS_PER_CALL, realTags) }),
      );
      const timer = new Timer();
      await aztecNode.getPrivateLogsByTags({ tags: tagsOfSize(TAGS_PER_CALL, realTags) });
      queuedTimings.push(timer.ms());
      await Promise.all(inFlight);
    }
    record('inproc_100_real_tags_queued_behind_5', calculateStats(queuedTimings));
  });
});
