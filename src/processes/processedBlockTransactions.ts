import { config } from "../config/config";
import kafkaHelper from "../config/kafka";
import { blockchainHelper } from "../helpers";
import { TxReceipt } from "../interfaces/global_helper";
import cosmosProcessHelper from "./process.helper";
import { TxType } from "../types/erc20Tokens.Types";
import { TrnxTypeEnum } from "../enum";
import { globalHelper } from "../helpers/global";
import { sha256 } from "@cosmjs/crypto"
import { toHex } from "@cosmjs/encoding"
import { DvpnSessionsInterface } from "../interfaces/global_helper";
class ProcessedBlockTransactions {
    public cosmos_block_tx_topic = config.KAFKA_BLOCKS.COSMOS_BLOCK_TX_TOPIC;
    public cosmos_block_tx_group_id = config.KAFKA_BLOCKS.COSMOS_BLOCK_TX_GROUP_ID;
    public cosmos_behind_block_tx_topic = config.KAFKA_BLOCKS.COSMOS_BEHIND_BLOCK_TX_TOPIC;
    public cosmos_behind_block_tx_group_id = config.KAFKA_BLOCKS.COSMOS_BEHIND_BLOCK_TX_GROUP_ID;
    public deposit_withdraw_topic = config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_TOPIC;
    public dvpn_session_topic = config.KAFKA_TRANSACTIONS.DVPN_SESSION_TOPIC;
    constructor() { }

    public startBlockTxQueue = async () => {
        let consumer: any = null;
        let isConnected = false;

        const connectConsumer = async () => {
            try {

                if (consumer) {
                    await consumer.disconnect();
                }

                consumer = await kafkaHelper.kafka.consumer({
                    groupId: this.cosmos_block_tx_group_id,
                    // Add session timeout for better error handling
                    sessionTimeout: 30000,
                    // Add heartbeat interval
                    heartbeatInterval: 3000,
                });

                await consumer.subscribe({
                    topic: this.cosmos_block_tx_topic,
                    fromBeginning: true, // ✅ Changed to false to prevent reprocessing
                });

                await consumer.run({
                    // ✅ Add proper autoCommit configuration
                    autoCommit: true,
                    autoCommitInterval: 5000,
                    autoCommitThreshold: 100,

                    eachMessage: async ({ topic, partition, message }: { topic: any; partition: any; message: any }) => {
                        try {
                            console.log("🚀 ~ startBlockTxQueue topic >>>>", topic);
                            console.log("🚀 ~ startBlockTxQueue message.value >>>>", message.value.toString());
                            this.readTrnxV2(JSON.parse(message.value.toString()))

                        } catch (error) {
                            console.error(`❌ Error processing message:`, error);
                        }
                    },
                });
                isConnected = true;
            } catch (error) {
                console.error(`❌ Failed to connect consumer:`, error);
                isConnected = false;
                throw error;
            }
        };

        // ✅ Add reconnection logic
        const startWithReconnection = async () => {
            while (true) {
                try {
                    if (!isConnected) {
                        console.log(`🔄 Attempting to connect consumer startBlockTxQueue...`);
                        await connectConsumer();
                    }

                    // Keep the consumer alive
                    await new Promise(resolve => setTimeout(resolve, 30000));

                } catch (error) {
                    console.error(`❌ Consumer connection error:`, error);
                    isConnected = false;

                    // Wait before retrying
                    console.log(`⏳ Waiting 10 seconds before reconnecting...`);
                    await new Promise(resolve => setTimeout(resolve, 10000));
                }
            }
        };

        // Start the consumer with reconnection
        await startWithReconnection();
    };

    public startBehindBlockTxQueue = async () => {
        let consumer: any = null;
        let isConnected = false;

        const connectConsumer = async () => {
            try {

                if (consumer) {
                    await consumer.disconnect();
                }

                consumer = await kafkaHelper.kafka.consumer({
                    groupId: this.cosmos_behind_block_tx_group_id,
                    // Add session timeout for better error handling
                    sessionTimeout: 30000,
                    // Add heartbeat interval
                    heartbeatInterval: 3000,
                });

                await consumer.subscribe({
                    topic: this.cosmos_behind_block_tx_topic,
                    fromBeginning: false, // ✅ Changed to false to prevent reprocessing
                });

                await consumer.run({
                    // ✅ Add proper autoCommit configuration
                    autoCommit: true,
                    autoCommitInterval: 5000,
                    autoCommitThreshold: 100,

                    eachMessage: async ({ topic, partition, message }: { topic: any; partition: any; message: any }) => {
                        try {
                            console.log("🚀 ~ startBehindBlockTxQueue topic >>>>", topic);
                            console.log("🚀 ~ startBehindBlockTxQueue message.value >>>>", message.value.toString());
                            this.readTrnxV2(JSON.parse(message.value.toString()))

                        } catch (error) {
                            console.error(`❌ Error processing message:`, error);
                        }
                    },
                });
                isConnected = true;
            } catch (error) {
                console.error(`❌ Failed to connect consumer:`, error);
                isConnected = false;
                throw error;
            }
        };

        // ✅ Add reconnection logic
        const startWithReconnection = async () => {
            console.log('isConnected >>>', isConnected);
            while (true) {
                try {
                    if (!isConnected) {
                        console.log(`🔄 Attempting to connect consumer startBehindBlockTxQueue...`);
                        await connectConsumer();
                    }

                    // Keep the consumer alive
                    await new Promise(resolve => setTimeout(resolve, 30000));

                } catch (error) {
                    console.error(`❌ Consumer connection error:`, error);
                    isConnected = false;

                    // Wait before retrying
                    console.log(`⏳ Waiting 10 seconds before reconnecting...`);
                    await new Promise(resolve => setTimeout(resolve, 10000));
                }
            }
        };

        // Start the consumer with reconnection
        await startWithReconnection();
    };

    private async readTrnxV2(data: any) {
        try {
            let computedTxHash = "";
            if (data.tx_base64) {
                const txRawBuffer = Buffer.from(data.tx_base64, 'base64');
                const hashBytes = sha256(txRawBuffer);
                computedTxHash = '0x' + toHex(hashBytes);
                data.tx_id = computedTxHash;
            } else {
                return;
            }
            // Use Tendermint REST API to fetch transaction by hash
            const txHash = data.tx_id.startsWith('0x') ? data.tx_id : '0x' + data.tx_id;
            const response = await blockchainHelper.fetchFromCosmosApi(config.NODE.COSMOS_RPC_URL, {
                method: "GET",
                headers: {
                    "Content-Type": "application/json",
                },
                params: {
                    path: `/tx?hash=${txHash}`,
                },
            });
            if (!response?.ok) {
                // Transaction not found, retry if under limit
                if (Number(data.retry_count) < 6) {
                    data.retry_count = Number(data.retry_count) + 1;
                    await kafkaHelper.produceMessage(this.cosmos_behind_block_tx_topic, data);
                }
                return false;
            }

            const txResponse = await response?.json();
            // Defensive: check for tx_result and code
            const txResult = txResponse?.result?.tx_result;
            if (txResult?.code === 0) {
                let trnxFee = 0;
                for (const event of txResult.events || []) {
                    // Transaction Fee
                    if (event.type === "tx") {
                        for (const attr of event.attributes) {
                            if (attr.key === "fee") {
                                trnxFee = Number(attr.value.replace("tics", "")) / 1e18;
                            }
                        }
                        break;
                    }
                }
                for (const log of JSON.parse(txResult?.log)) {
                    for (const event of log.events || []) {
                        // Normal Transfers
                        if (event.type === "transfer") {
                            let sender, recipient, amt;
                            for (const attr of event.attributes) {
                                if (attr.key === "sender") sender = attr.value;
                                if (attr.key === "recipient") recipient = attr.value;
                                if (attr.key === "amount") amt = attr.value.replace("tics", "");
                            }

                            // Ignore module/validator accounts
                            if (recipient?.startsWith("qubeticsvaloper") || recipient?.startsWith("qubetics1fl")) {
                                continue;
                            }

                            let finalAmount = "0";
                            if (amt) {
                                finalAmount = (Number(amt) / 1e18).toString();
                            }
                            // Compose a pseudo-TransactionReceipt for Cosmos
                            const txReceipt: TxReceipt = {
                                txHash: txHash,
                                blockNumber: data.block_number,
                                from: sender,
                                to: recipient,
                                value: finalAmount,
                                trnxFee: trnxFee

                                // logs: [], // Not EVM logs, but we can pass empty or custom if needed
                                // gasUsed: txResult?.gas_used || 0,
                                // gasPrice: txResult?.gas_price || 1, // Not available in Cosmos, set to 0 or estimate if needed
                            };
                            console.log('transfer txReceipt >>>', txReceipt);

                            await processedQueueTransactions.checkCosmosDeposit(txReceipt);

                        }

                        // Staking Delegation
                        if (event.type === "delegate") {
                            let delegator, validator, amt;
                            for (const attr of event.attributes) {
                                if (attr.key === "delegator") delegator = attr.value;
                                if (attr.key === "validator") validator = attr.value;
                                if (attr.key === "amount") amt = attr.value.replace("tics", "");
                            }
                            let finalAmount = "0";
                            if (amt) {
                                finalAmount = (Number(amt) / 1e18).toString();
                            }
                            const txReceipt: TxReceipt = {
                                txHash: txHash,
                                blockNumber: data.block_number,
                                from: delegator,
                                to: validator,
                                value: finalAmount,
                                trnxFee: trnxFee

                            };
                            console.log('delegate txReceipt >>>', txReceipt);

                            await processedQueueTransactions.checkCosmosDeposit(txReceipt);
                        }

                        // Unstaking
                        if (event.type === "unbond") {
                            let delegator, validator, amt;
                            for (const attr of event.attributes) {
                                if (attr.key === "delegator") delegator = attr.value;
                                if (attr.key === "validator") validator = attr.value;
                                if (attr.key === "amount") amt = attr.value.replace("tics", "");
                            }
                            let finalAmount = "0";
                            if (amt) {
                                finalAmount = (Number(amt) / 1e18).toString();
                            }
                            const txReceipt: TxReceipt = {
                                txHash: txHash,
                                blockNumber: data.block_number,
                                from: delegator,
                                to: validator,
                                value: finalAmount,
                                trnxFee: trnxFee

                            };
                            console.log('unbond txReceipt >>>', txReceipt);
                            await processedQueueTransactions.checkCosmosDeposit(txReceipt);
                        }

                        // DVPN Session Update Status
                        if (event?.type == "qubetics.session.v3.EventUpdateStatus") {

                            let acc_address = "", id = "", node_address = "", status = "", status_at = "";
                            for (const attr of event.attributes) {
                                const cleanValue = attr.value?.replace(/"/g, "") || "";
                                if (attr.key === "acc_address") acc_address = cleanValue || "";
                                if (attr.key === "id") id = cleanValue || "";
                                if (attr.key === "node_address") node_address = cleanValue || "";
                                if (attr.key === "status") status = cleanValue || "";
                                if (attr.key === "status_at") status_at = cleanValue || "";

                            }

                            const sessionDetails: DvpnSessionsInterface = {
                                wallet_address: acc_address?.toLowerCase() || "",
                                node_id: id?.toString() || "",
                                node_address: node_address?.toLowerCase() || "",
                                status: status || "",
                                status_at: status_at || "",
                                block: Number(data.block_number) || 0,
                                event: event.type || "",
                                tx_hash: txHash || ""
                            };

                            console.log('DVPN Event Update Status sessionDetails 🚅🚅>>>', sessionDetails);

                            await processedQueueTransactions.checkOurDvpnSession(sessionDetails);


                        }

                        //// DVPN Session Create Session
                        if (event?.type == "qubetics.subscription.v3.EventCreateSession") {

                            let acc_address = "", id = "", node_address = "", status = "", status_at = "";
                            for (const attr of event.attributes) {
                                if (attr.key === "acc_address") acc_address = attr.value || "";
                                if (attr.key === "id") id = attr.value || "";
                                if (attr.key === "node_address") node_address = attr.value || "";
                                if (attr.key === "status") status = attr.value || "";
                                if (attr.key === "status_at") status_at = attr.value || "";
                            }

                            const sessionDetails: DvpnSessionsInterface = {
                                wallet_address: acc_address?.toLowerCase() || "",
                                node_id: id?.toString() || "",
                                node_address: node_address?.toLowerCase() || "",
                                status: status || "",
                                status_at: status_at || "",
                                block: Number(data.block_number) || 0,
                                event: event.type || "",
                                tx_hash: txHash || ""
                            };

                            console.log('DVPN Event Create Session 🚅🚅>>>', sessionDetails);

                            await processedQueueTransactions.checkOurDvpnSession(sessionDetails);

                        }

                        if (event?.type == "qubetics.subscription.v3.EventCreate") {
                            let acc_address = "", id = "", plan_id = "", price = "", prov_address = "";
                            for (const attr of event.attributes) {
                                if (attr.key === "acc_address") acc_address = attr.value || "";
                                if (attr.key === "id") id = attr.value || "";
                                if (attr.key === "plan_id") plan_id = attr.value || "";
                                if (attr.key === "price") price = attr.value || "";
                                if (attr.key === "prov_address") prov_address = attr.value || "";
                            }

                            const extraData = {
                                plan_id: plan_id || "",
                                price: price || "",
                                prov_address: prov_address || ""
                            }

                            const sessionDetails: DvpnSessionsInterface = {
                                wallet_address: acc_address?.toLowerCase() || "",
                                node_id: id?.toString() || "",
                                node_address: "",
                                status: "",
                                status_at: "",
                                block: Number(data.block_number) || 0,
                                event: event.type || "",
                                tx_hash: txHash || "",
                                extra_data: extraData
                            }

                            console.log('DVPN Event Create (Purchase Subscription) sessionDetails 🚅🚅>>>', sessionDetails);

                            await processedQueueTransactions.checkOurDvpnSession(sessionDetails);

                        }


                    }
                }


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

    private async checkCosmosDeposit(txReceipt: TxReceipt) {
        try {
            /** for check the eth deposit */
            if (
                txReceipt.value !== '0' &&
                txReceipt.to &&
                txReceipt.blockNumber
            ) {

                const amount = parseFloat(txReceipt.value);

                const txData: TxType = {
                    txId: txReceipt.txHash,
                    fromAddress: txReceipt.from.toLowerCase(),
                    toAddress: txReceipt.to.toLowerCase(),
                    token: null,
                    amount,
                    blockId: txReceipt?.blockNumber,
                    isNFTToken: false,
                    txType: null,
                    trnx_fee: Number(txReceipt.trnxFee)
                };
                await this.checkAddressInDB(txData);
            }
        } catch (error: any) {
            console.error("💥 ~ ~ check_eth_deposit error", (error as Error).message);
            await globalHelper.save_error_logs('COSMOS_check_deposit', error.message)

        }
    }


    private async checkAddressInDB(txData: TxType) {
        try {

            const addresses = [txData.fromAddress, txData.toAddress].filter(Boolean);

            if (addresses.length > 0) {
                const results = await cosmosProcessHelper.getMultipleKeyValuePairs(`${config.COSMOS_WALLET_ADDRESS}`, addresses);

                if (results) {
                    const [withdrawTx, depositTx] = results as string[];
                    if (depositTx) {
                        txData.txType = TrnxTypeEnum.DEPOSIT;
                    } else if (withdrawTx) {
                        txData.txType = TrnxTypeEnum.WITHDRAW;
                    }
                }
            }
            if (txData.txType !== null) {
                await processedQueueTransactions.addTxToQueue(
                    txData,
                    this.deposit_withdraw_topic || ""
                );
            }
        } catch (err: any) {
            console.error("Error in checkAddressInDB>>", err)
        }
    }

    private async addTxToQueue(data: TxType, queueName: string) {
        try {
            await kafkaHelper.produceMessage(queueName, data)
        } catch (err: any) {
            console.error("addTxToQueue", (err as Error).message);
            await globalHelper.save_error_logs('COSMOS_checkAddressInDB', err.message)

        }
    };


    private async checkOurDvpnSession(sessionDetails: DvpnSessionsInterface) {
        try {
            let { wallet_address } = sessionDetails;
            if (!wallet_address) return;

            wallet_address = wallet_address.toUpperCase();
            wallet_address = wallet_address.replace(/^"|"$/g, '');

            // Check if the wallet address present in REDIS
            const walletData = await cosmosProcessHelper.checkOurWalletAddress(wallet_address);

            if (!walletData) {
                console.log("Not OUR Qubetics Native Wallet Returning 📢📢 >>>", wallet_address);
                return;

            }


            const { wallet_id, user_id } = walletData;

            // Merge wallet_id and user_id into sessionDetails
            const enrichedSession = { ...sessionDetails, wallet_id, user_id };

            // Send to Kafka
            await kafkaHelper.produceMessage(
                config.KAFKA_TRANSACTIONS.DVPN_SESSION_TOPIC,
                enrichedSession
            );
        } catch (err) {
            console.error("❌ Error in checkOurDvpnSession:", err);
        }
    }

}
export const processedQueueTransactions = new ProcessedBlockTransactions()
