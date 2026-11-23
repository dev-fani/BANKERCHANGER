import { PrismaClient } from '@prisma/client';
import { SorobanRpc, xdr, scValToNative } from '@stellar/stellar-sdk';
import { logger } from '../utils/logger';

const prisma = new PrismaClient();

interface WinningsClaimedEvent {
  bettor_address: string;
  market_id: number;
  amount_won: bigint;
  fee_deducted: bigint;
  claimed_at: number;
}

export class StellarIndexer {
  private server: SorobanRpc.Server;
  private contractId: string;

  constructor(rpcUrl: string, contractId: string) {
    this.server = new SorobanRpc.Server(rpcUrl);
    this.contractId = contractId;
  }

  async handleWinningsClaimed(event: WinningsClaimedEvent): Promise<void> {
    const { bettor_address, market_id, amount_won, claimed_at } = event;

    const claimedAt = new Date(claimed_at * 1000);

    await prisma.bet.updateMany({
      where: {
        bettor_address,
        market_id,
        claimed: false,
      },
      data: {
        claimed: true,
        claimed_at: claimedAt,
        payout: amount_won,
      },
    });

    logger.info(
      `WinningsClaimed processed for bettor ${bettor_address} on market ${market_id}`
    );
  }

  async processEvent(event: SorobanRpc.Api.EventResponse): Promise<void> {
    const topics = event.topic.map((t) => scValToNative(t));
    const eventName = topics[0];
    const data = scValToNative(event.value) as Record<string, unknown>;

    switch (eventName) {
      case 'WinningsClaimed':
        await this.handleWinningsClaimed({
          bettor_address: String(data.bettor_address),
          market_id: Number(data.market_id),
          amount_won: BigInt(String(data.amount_won)),
          fee_deducted: BigInt(String(data.fee_deducted)),
          claimed_at: Number(data.claimed_at),
        });
        break;
      default:
        logger.debug(`Unhandled event: ${String(eventName)}`);
    }
  }

  /**
   * Backfill historical ledger ranges in ascending order, processing in
   * bounded batches to avoid OOM. Events are deduplicated via ON CONFLICT
   * DO UPDATE, and the checkpoint is only advanced after the final ledger.
   */
  async backfillLedgerRange(
    fromLedger: number,
    toLedger: number,
    batchSize: number
  ): Promise<void> {
    if (fromLedger > toLedger) {
      throw new Error(
        `Invalid ledger range: fromLedger (${fromLedger}) > toLedger (${toLedger})`
      );
    }
    if (batchSize <= 0) {
      throw new Error(`Invalid batchSize: ${batchSize}`);
    }

    logger.info(
      `Starting backfill from ledger ${fromLedger} to ${toLedger} (batchSize=${batchSize})`
    );

    let processed = 0;

    for (let start = fromLedger; start <= toLedger; start += batchSize) {
      const end = Math.min(start + batchSize - 1, toLedger);

      // Fetch events for this bounded ledger window only, keeping memory
      // usage proportional to batchSize rather than the full range.
      const events = await this.fetchEventsForRange(start, end);

      for (const event of events) {
        await this.persistEventDeduped(event);
      }

      processed += end - start + 1;

      if (processed % 1000 === 0 || end === toLedger) {
        logger.info(
          `Backfill progress: processed up to ledger ${end} (${processed} ledgers)`
        );
      }
    }

    // Checkpoint is only updated after the final ledger has been processed.
    await this.updateCheckpoint(toLedger);

    logger.info(`Backfill complete. Checkpoint updated to ledger ${toLedger}`);
  }

  private async fetchEventsForRange(
    startLedger: number,
    endLedger: number
  ): Promise<SorobanRpc.Api.EventResponse[]> {
    const events: SorobanRpc.Api.EventResponse[] = [];
    let cursor: string | undefined;

    do {
      const response = await this.server.getEvents({
        startLedger,
        filters: [
          {
            type: 'contract',
            contractIds: [this.contractId],
          },
        ],
        cursor,
        limit: 200,
      });

      for (const event of response.events) {
        if (event.ledger <= endLedger) {
          events.push(event);
        }
      }

      cursor = response.cursor;
      if (response.events.length === 0) {
        break;
      }
    } while (cursor);

    return events;
  }

  private async persistEventDeduped(
    event: SorobanRpc.Api.EventResponse
  ): Promise<void> {
    const topics = event.topic.map((t) => scValToNative(t));
    const eventName = String(topics[0]);
    const data = scValToNative(event.value) as Record<string, unknown>;

    // Deduplicate on the unique (tx_hash, event_index) key so re-running a
    // backfill never inserts duplicate rows.
    await prisma.$executeRaw`
      INSERT INTO "IndexedEvent" ("tx_hash", "event_index", "ledger", "event_name", "payload", "created_at")
      VALUES (${event.txHash}, ${event.id}, ${event.ledger}, ${eventName}, ${JSON.stringify(
        data,
        (_k, v) => (typeof v === 'bigint' ? v.toString() : v)
      )}::jsonb, NOW())
      ON CONFLICT ("tx_hash", "event_index") DO UPDATE
      SET "ledger" = EXCLUDED."ledger",
          "event_name" = EXCLUDED."event_name",
          "payload" = EXCLUDED."payload"
    `;
  }

  private async updateCheckpoint(ledger: number): Promise<void> {
    await prisma.$executeRaw`
      INSERT INTO "IndexerCheckpoint" ("id", "last_ledger", "updated_at")
      VALUES ('default', ${ledger}, NOW())
      ON CONFLICT ("id") DO UPDATE
      SET "last_ledger" = EXCLUDED."last_ledger",
          "updated_at" = EXCLUDED."updated_at"
    `;
  }
}
