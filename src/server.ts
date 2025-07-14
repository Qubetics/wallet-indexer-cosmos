import { cosmosWithdrawDepositProcess, cosmosPendingWithdrawalProcess, cosmosTxStatusUpdateProcess, cosmosBlocksProcess, cosmos_tx_status_deposit_withdraw, Cosmos_balance_update } from "./processes/index.process"
import schedule from "node-schedule";
class CosmosStartProcess {
  constructor() {
    // this.read_block_process()
    this.readSpecificBlock()
    // this.consume_read_block_queue()
    // this.cosmosWithdrawDepositProcess();
    // this.getPendingWithdrawalTxProcess(); //DONE AND CHANGED
    // this.updateTxStatusProcess();
    // this.CosmosUpdateBalance();
    // this.testing_node()
  }
  public read_block_process() {
    schedule.scheduleJob("*/8 * * * * *", async function () {
      await cosmosBlocksProcess.getBlocks();
      // await cosmosBlocksProcess.readBehindBlock()
    });
  }
  // // public testing_node() {
  // //   schedule.scheduleJob("*/2 * * * * *", async function () {
  // //     await eth_blocks_process.testingNode();
  // //   });
  // // }
  public readSpecificBlock() {
    schedule.scheduleJob('*/1 * * * *', async function () {
      await cosmosBlocksProcess.readSpecificBlock();
    })
  }
  public consume_read_block_queue() {
    setTimeout(async () => {
      await cosmos_tx_status_deposit_withdraw.startBlockTxQueue();
    //   await cosmos_tx_status_deposit_withdraw.startBehindBlockTxQueue();
    }, 10000);
  }
  public async cosmosWithdrawDepositProcess() {
    setTimeout(async () => {
      await cosmosWithdrawDepositProcess.startDepositQueue();
    }, 10000);
  }
  public getPendingWithdrawalTxProcess() {
    setTimeout(async () => {
      await cosmosPendingWithdrawalProcess.getTransactionFromDB();
    });
  }
  public async updateTxStatusProcess() {
    setTimeout(async () => {
      await cosmosTxStatusUpdateProcess.startTxStatusUpdateQueue();
    }, 10000);
  }
  public async CosmosUpdateBalance() {
    setTimeout(async () => {
      await Cosmos_balance_update.default.update_wallet_balance();
    }, 10000);
  }
}

new CosmosStartProcess();
