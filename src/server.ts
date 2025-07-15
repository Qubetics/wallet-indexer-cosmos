import kafkaHelper from "./config/kafka.helper";
import { cosmosWithdrawDepositProcess, cosmosPendingWithdrawalProcess, cosmosTxStatusUpdateProcess, cosmosBlocksProcess, cosmosTxStatusDepositWithdraw, cosmosBalanceUpdate } from "./processes/index.process"
import schedule from "node-schedule";
class CosmosStartProcess {
  constructor() {
    this.connectKafka();
    this.readBlockProcess()
    this.readSpecificBlock()
    this.consumeReadBlockQueue()
    this.consumeReadBehindBlockQueue()
    this.cosmosWithdrawDepositProcess();
    this.getPendingWithdrawalTx();
    this.updateTxStatus();
    this.cosmosUpdateBalance();
  }

  public async connectKafka() {
    await kafkaHelper.connect();
  }

  public readBlockProcess() {
    schedule.scheduleJob("*/8 * * * * *", async function () {
      await cosmosBlocksProcess.getBlocks();
      await cosmosBlocksProcess.readBehindBlock()
    });
  }
  public readSpecificBlock() {
    schedule.scheduleJob('*/1 * * * *', async function () {
      await cosmosBlocksProcess.readSpecificBlock();
    })
  }
  public consumeReadBlockQueue() {
    setTimeout(async () => {
      await cosmosTxStatusDepositWithdraw.startBlockTxQueue();
    }, 10000);
  }
  public consumeReadBehindBlockQueue() {
    setTimeout(async () => {
      await cosmosTxStatusDepositWithdraw.startBehindBlockTxQueue();
    }, 10000);
  }
  public async cosmosWithdrawDepositProcess() {
    setTimeout(async () => {
      await cosmosWithdrawDepositProcess.startDepositQueue();
    }, 10000);
  }
  public getPendingWithdrawalTx() {
    setTimeout(async () => {
      await cosmosPendingWithdrawalProcess.getTransactionFromDB();
    });
  }
  public async updateTxStatus() {
    setTimeout(async () => {
      await cosmosTxStatusUpdateProcess.startTxStatusUpdateQueue();
    }, 10000);
  }
  public async cosmosUpdateBalance() {
    setTimeout(async () => {
      await cosmosBalanceUpdate.default.updateWalletBalance();
    }, 10000);
  }
}

new CosmosStartProcess();
