import { config } from "../config/config";
import kafkaHelper from "../config/kafka.helper";
import { Blockchain_Helper, Utility_Helper } from "../helpers";
import { blocks_trnx } from "../interfaces/global_helper";
import cosmosProcessHelper from "./process.helper";
import { TxType } from "../types/ERC20Tokens.types";
import { TrnxTypeEnum } from "../enum";
import { global_helper } from "../helpers/global_helper";
import { TransactionReceipt } from "ethers";
import fetch from 'node-fetch';
class queue_transactions {
    public cosmos_block_tx_topic = config.KAFKA_BLOCKS.COSMOS_BLOCK_TX_TOPIC;
    public cosmos_block_tx_group_id = config.KAFKA_BLOCKS.COSMOS_BLOCK_TX_GROUP_ID;
    public cosmos_behind_block_tx_topic = config.KAFKA_BLOCKS.COSMOS_BEHIND_BLOCK_TX_TOPIC;
    public cosmos_behind_block_tx_group_id = config.KAFKA_BLOCKS.COSMOS_BEHIND_BLOCK_TX_GROUP_ID;
    public deposit_withdraw_topic = config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_TOPIC;

    constructor() {}

    public startBlockTxQueue = async () => {
        let consumer = await kafkaHelper.kafka.consumer({ groupId: this.cosmos_block_tx_group_id });
        await consumer.subscribe({ topic: this.cosmos_block_tx_topic, fromBeginning: true });

        await consumer.run({
            eachMessage: ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
                this.read_trnx(JSON.parse(message.value.toString()))
            }
        }).catch((err: Error) => {
            console.log("🔥 ~ startBlockTxQueue consumer error", err);
        });
    };

    public startBehindBlockTxQueue = async () => {
        let consumer = await kafkaHelper.kafka.consumer({ groupId: this.cosmos_behind_block_tx_group_id });
        await consumer.subscribe({ topic: this.cosmos_behind_block_tx_topic, fromBeginning: true });

        await consumer.run({
            eachMessage: ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
                this.read_trnx(JSON.parse(message.value.toString()))
            }
        }).catch((err: Error) => {
            console.log("🔥 ~ startBehindBlockTxQueue consumer error", err);
        });
    };
    public async read_trnx(data: blocks_trnx) {
        try {
            console.log("🚀 ~ queue_transactions ~ read_trnx ~ data:", data)
            // Use Tendermint REST API to fetch transaction by hash
            const txHash = data.tx_id.startsWith('0x') ? data.tx_id : '0x' + data.tx_id;
            const url = `https://tendermint-internal.qubetics.com/tx?hash=${txHash}`;
            const response = await fetch(url);

            if (!response.ok) {
                // Transaction not found, retry if under limit
                if (Number(data.retry_count) < 6) {
                    data.retry_count = Number(data.retry_count) + 1;
                    await kafkaHelper.produceMessage(this.cosmos_behind_block_tx_topic, data);
                }
                return false;
            }

            const txResponse = await response.json();
            console.log("🚀 ~ queue_transactions ~ read_trnx ~ txResponse:", txResponse)

            // Defensive: check for tx_result and code
            const txResult = txResponse?.tx_result;
            if (txResult?.code === 0) {
                // Parse Cosmos events to extract transfer details
                let fromAddress = "";
                let toAddress = "";
                let amount = "0";
                let trnx_fee = 0;

                // Find transfer event and extract sender, recipient, amount
                if (Array.isArray(txResult.events)) {
                    for (const event of txResult.events) {
                        if (event.type === "transfer" && Array.isArray(event.attributes)) {
                            for (const attr of event.attributes) {
                                if (attr.key === "sender") {
                                    fromAddress = attr.value;
                                }
                                if (attr.key === "recipient") {
                                    toAddress = attr.value;
                                }
                                if (attr.key === "amount" && attr.value) {
                                    // attr.value is like "2000000000000000000tics"
                                    amount = attr.value.replace('tics', '');
                                }
                            }
                        }
                        // Also try to extract fee from tx event
                        if (event.type === "tx" && Array.isArray(event.attributes)) {
                            for (const attr of event.attributes) {
                                if (attr.key === "fee" && attr.value) {
                                    const feeStr = attr.value.replace('tics', '');
                                    trnx_fee = Number(feeStr) / 1e18; // convert to TICS (assuming 18 decimals)
                                }
                            }
                        }
                    }
                }

                // Fallback: try to get sender from message event if not found
                if (!fromAddress && Array.isArray(txResult.events)) {
                    for (const event of txResult.events) {
                        if (event.type === "message" && Array.isArray(event.attributes)) {
                            for (const attr of event.attributes) {
                                if (attr.key === "sender" && attr.value) {
                                    fromAddress = attr.value;
                                    break;
                                }
                            }
                        }
                        if (fromAddress) break;
                    }
                }

                // Fallback: try to get recipient from coin_received event if not found
                if (!toAddress && Array.isArray(txResult.events)) {
                    for (const event of txResult.events) {
                        if (event.type === "coin_received" && Array.isArray(event.attributes)) {
                            for (const attr of event.attributes) {
                                if (attr.key === "receiver" && attr.value) {
                                    toAddress = attr.value;
                                    break;
                                }
                            }
                        }
                        if (toAddress) break;
                    }
                }

                // Defensive: convert amount to TICS (float)
                let finalAmount = "0";
                if (amount) {
                    finalAmount = (Number(amount) / 1e18).toString();
                }

                // Compose a pseudo-TransactionReceipt for Cosmos
                const txReceipt: any = {
                    status: 1,
                    transactionHash: txHash,
                    blockNumber: data.block_number,
                    logs: [], // Not EVM logs, but we can pass empty or custom if needed
                    gasUsed: txResult?.gas_used || 0,
                    gasPrice: 0, // Not available in Cosmos, set to 0 or estimate if needed
                    from: fromAddress,
                    to: toAddress,
                    value: finalAmount,
                    trnx_fee: trnx_fee
                };

                // Optionally, you could pass from/to/value/trnx_fee in data for downstream
                data.fromAddress = fromAddress;
                data.toAddress = toAddress;
                data.value = finalAmount;

                await cosmos_tx_status_deposit_withdraw.check_cosmos_deposit(data, txReceipt);
            } else if (txResult?.code !== undefined) {
                // Transaction failed
                console.log("(tx_result.code !== 0) Transaction failed for hash:", txHash);
            } else {
                // Transaction not found or unknown status, retry if under limit
                if (Number(data.retry_count) < 6) {
                    data.retry_count = Number(data.retry_count) + 1;
                    await kafkaHelper.produceMessage(this.cosmos_behind_block_tx_topic, data);
                }
                return false;
            }
        } catch (error: any) {
            console.error("Error in read_trnx>>>", error);
            if (Number(data.retry_count) < 3) {
                data.retry_count = Number(data.retry_count) + 1;
                console.log("Adding transaction in queue>>>", data.retry_count, "transaction id>>>", data);
                await kafkaHelper.produceMessage(this.cosmos_behind_block_tx_topic, data);
            }
        }
    }
    // public async buildTx(txReceipt: TransactionReceipt, data: blocks_trnx) {
    //     try {
    //         // Cosmos txReceipt.logs is always empty or not EVM logs, so we skip EVM log parsing
    //         // Instead, we use the data and txReceipt fields directly for Cosmos
    //         // Only check_eth_deposit for Cosmos
    //         await cosmos_tx_status_deposit_withdraw.check_cosmos_deposit(data, txReceipt);
    //     } catch (error: any) {
    //         console.error("buildTx error >>>", (error as Error).message);
    //         await global_helper.save_error_logs('Cosmos_buildTx', error.message)
    //     }
    // };
    public async check_cosmos_deposit(data: blocks_trnx, txReceipt: TransactionReceipt) {
        try {
            /** for check the eth deposit */
            if (
                data.value !== '0' &&
                data.toAddress &&
                data.block_number
            ) {
                const gasUsed = txReceipt.gasUsed ? txReceipt.gasUsed : 0;
                const effectiveGasPrice: number = Number(txReceipt.gasPrice) || 0;
                const txFee = await Utility_Helper.bigNumberSafeMath(gasUsed, '*', effectiveGasPrice);
                const gasInCosmos = Blockchain_Helper.convertToWei(txFee);

                const cosmosValue = Blockchain_Helper.convertToWei((data.value ? data.value : 0));
                const amount = parseFloat(cosmosValue.toString());

                const txData: TxType = {
                    txId: data.tx_id,
                    fromAddress: data.fromAddress.toLowerCase(),
                    toAddress: data.toAddress.toLowerCase(),
                    token: null,
                    amount,
                    blockId: data?.block_number,
                    isNFTToken: false,
                    txType: TrnxTypeEnum.NULL,
                    trnx_fee: Number(gasInCosmos)
                };
                await this.checkAddressInDB(txData);
            }
        } catch (error: any) {
            console.error("💥 ~ ~ check_eth_deposit error", (error as Error).message);
            await global_helper.save_error_logs('COSMOS_check_deposit', error.message)

        }
    }
    public async checkAddressInDB(txData: TxType) {
        try {
            let ourTrnx: any = null;
            const deposit = await cosmosProcessHelper.check_our_wallet_address(txData.toAddress);
            if (deposit) {
                console.log("Found the user in to (DEPOSIT) tx 😸");
                txData.txType = TrnxTypeEnum.DEPOSIT;
                ourTrnx = txData;
            }else{
                const withdraw = await cosmosProcessHelper.check_our_wallet_address(txData.fromAddress);
                if (withdraw) {
                    console.log("Found the user in FROM (WITHDRAW) tx 😸");
                    txData.txType = TrnxTypeEnum.WITHDRAW;
                    ourTrnx = txData;
                }
            }

            
           
            if (ourTrnx) {
                await cosmos_tx_status_deposit_withdraw.addTxToQueue(
                    ourTrnx,
                    this.deposit_withdraw_topic || ""
                );
            }
        } catch (err: any) {
            console.error("Error in checkAddressInDB>>", err)
        }
    }
    // public getTransferEventDetail = async (
    //     tokenAddress: string,
    //     blockId: number
    // ) => {
    //     try {
    //         const contract = new ethers.Contract(
    //             tokenAddress,
    //             config.ERC721ABI,
    //             Blockchain_Helper.provider
    //         );

    //         const filter = contract.filters.Transfer();
    //         const events = await contract.queryFilter(
    //             filter,
    //             blockId,
    //             blockId
    //         );

    //         return events;
    //     } catch (error) {
    //         console.log("🔥 ~ getTransferEventDetail error", (error as Error).message);
    //         return [];
    //     }
    // };
    public async addTxToQueue(data: TxType, queueName: string) {
        try {
            await kafkaHelper.produceMessage(queueName, data)
        } catch (err: any) {
            console.error("addTxToQueue", (err as Error).message);
            await global_helper.save_error_logs('COSMOS_checkAddressInDB', err.message)

        }
    };

}
export const cosmos_tx_status_deposit_withdraw = new queue_transactions()
