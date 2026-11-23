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
}
