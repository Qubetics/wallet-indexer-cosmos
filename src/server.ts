import kafkaHelper from "./config/kafka";
import { procesesDepositWithdrawQueue, processedInternalTransactions, updateInteralTransactionStatus, processedBlocks, processedQueueTransactions, processedAddressBalance, processedDvpnSession } from "./processes/index.process"
import schedule from "node-schedule";
class CosmosStartProcess {
  constructor() {
    this.connectKafka();
    this.readBlocks()
    this.readSpecificBlock()
    this.consumeReadBlockQueue()
    this.consumeReadBehindBlockQueue()
    this.cosmosWithdrawDepositProcess();
    this.addInternalTransactionsInQueue();
    this.updateInternalTransactionsStatus();
    this.cosmosUpdateBalance();

    this.consumeDvpnSessions()
  }

  public async connectKafka() {
    await kafkaHelper.connect();
  }

  public readBlocks() {
    schedule.scheduleJob("*/4 * * * * *", async function () {
      await processedBlocks.getBlocks();
      await processedBlocks.readBehindBlock()
    });
  }
  public readSpecificBlock() {
    schedule.scheduleJob('*/30 * * * * *', async function () {
      await processedBlocks.readSpecificBlock();
    })
  }
  public consumeReadBlockQueue() {
    setTimeout(async () => {
      await processedQueueTransactions.startBlockTxQueue();
    }, 10000);
  }
  public consumeReadBehindBlockQueue() {
    setTimeout(async () => {
      await processedQueueTransactions.startBehindBlockTxQueue();
    }, 10000);
  }
  public async cosmosWithdrawDepositProcess() {
    setTimeout(async () => {
      await procesesDepositWithdrawQueue.startDepositQueue();
    }, 10000);
  }
  public addInternalTransactionsInQueue() {
    schedule.scheduleJob("*/10 * * * * *", async function () {
      await processedInternalTransactions.getInternalPendingTransactions();
    });
  }
  public async updateInternalTransactionsStatus() {
    setTimeout(async () => {
      await updateInteralTransactionStatus.consumeTxStatusUpdateQueue();
    }, 10000);
  }
  public async cosmosUpdateBalance() {
    setTimeout(async () => {
      await processedAddressBalance.default.consumeWalletBalanceUpdateQueue();
    }, 10000);
  }

  public async consumeDvpnSessions() {
    setTimeout(async () => {
      await processedDvpnSession.consumeDvpnSessionsQueue();
    }, 1000);
  }
}

new CosmosStartProcess();
