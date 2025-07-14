import { config } from "../config/config";
import cosmosProcessHelper from "./process.helper";
import { KEYS } from "../enum";
import { blocks_trnx } from "../interfaces/global_helper";
import { TicsNativeOldBlockModel } from "../models";
import { global_helper } from "../helpers/global_helper";
import kafkaHelper from "../config/kafka.helper";
import fetch from 'node-fetch';
class CosmosBlockProcess {
    public minBlockConfirmation: number;
    public swap: string | null;
    public approval: string | null;
    public cosmos_block_tx_topic = config.KAFKA_BLOCKS.COSMOS_BLOCK_TX_TOPIC;
    public cosmos_behind_block_tx_topic = config.KAFKA_BLOCKS.COSMOS_BEHIND_BLOCK_TX_TOPIC;

    constructor() {
        this.minBlockConfirmation = Number(config.MIN_BLOCK_CONFORMATIONS);
    }

    public async getBlocks() {
        try {
            let old_block: any = await cosmosProcessHelper.getKeyValuePair(config.REDISKEYS.COSMOS_BLOCKS, config.REDISKEYS.OLD_BLOCK_COSMOS)
            let new_block: any = await cosmosProcessHelper.getKeyValuePair(config.REDISKEYS.COSMOS_BLOCKS, config.REDISKEYS.NEW_BLOCK_COSMOS)
            if (!old_block) {
                console.log("No block present in old block")
                await cosmosProcessHelper.setBlocks(config.REDISKEYS.COSMOS_BLOCKS, config.REDISKEYS.OLD_BLOCK_COSMOS, this.minBlockConfirmation, null)
            }
            if (!new_block) {
                console.log("No block present in new block")
                await cosmosProcessHelper.setBlocks(config.REDISKEYS.COSMOS_BLOCKS, config.REDISKEYS.NEW_BLOCK_COSMOS, this.minBlockConfirmation, null)
            }
            console.log("🚀 ~ CosmosBlockProcess ~ getBlocks ~ new_block:", new_block)
            console.log("🚀 ~ CosmosBlockProcess ~ getBlocks ~ old_block:", old_block)
            if (old_block && new_block) {
                let oldBlock: number = Number(old_block);
                let newBlock = Number(new_block);
                if (newBlock <= oldBlock) {
                    await cosmosProcessHelper.setBlocks(config.REDISKEYS.COSMOS_BLOCKS, config.REDISKEYS.NEW_BLOCK_COSMOS, this.minBlockConfirmation, newBlock)
                } else {
                    let block_diff: number = newBlock - oldBlock;
                    if (block_diff > 50) {
                        await cosmosProcessHelper.diff_blocks_add_in_db(newBlock, oldBlock)
                        await cosmosProcessHelper.setKeyValuePair(config.REDISKEYS.COSMOS_BLOCKS, config.REDISKEYS.OLD_BLOCK_COSMOS, newBlock.toString())
                        await cosmosProcessHelper.setBlocks(config.REDISKEYS.COSMOS_BLOCKS, config.REDISKEYS.NEW_BLOCK_COSMOS, this.minBlockConfirmation, newBlock)
                    } else {
                        await this.readBlocks(oldBlock, newBlock)
                    }
                }
            } else {
                console.log(" no old block and no new block")
                await global_helper.save_error_logs('cosmos_setBlocks', 'No old block no new block')
            }
        } catch (err: any) {
            console.error("Error in getBlocks", err)
            await global_helper.save_error_logs('cosmos_getBlocks', err.message)
        }
    }
    public async readBlocks(blockNumber: number, toBlockNumber: number) {
        try {
            let block_updated: number = blockNumber + 1;
            await cosmosProcessHelper.setKeyValuePair(config.REDISKEYS.COSMOS_BLOCKS, config.REDISKEYS.OLD_BLOCK_COSMOS, block_updated.toString());
            console.table({
                LATEST_BLOCK: blockNumber,
                BLOCK_DIFFERENCE: toBlockNumber - blockNumber,
                TO_BLOCK_NUMBER: toBlockNumber
            });
            await this.getTxFromBlock(blockNumber, KEYS.latestBlock);
        } catch (err: any) {
            console.error("Error in readBlocks>>.", (err as Error).message)
            await global_helper.save_error_logs('cosmos_readBlocks', err.message)
        }
    }
    public async getTxFromBlock(blockId: number, key: string) {
        try {
            // Use the Cosmos REST API to fetch block transactions
            const url = `https://swagger-internal.qubetics.com/cosmos/tx/v1beta1/txs/block/${blockId}`;
            const response = await fetch(url);
            if (!response.ok) {
                console.log("No block info from Cosmos REST API");
                await TicsNativeOldBlockModel.create({
                    block_number: blockId,
                    coin_family: config.STATIC_COIN_FAMILY.TICS,
                    start_block: blockId,
                    end_block: blockId,
                    status: 0
                });
                return;
            }
            const blockInfo = await response.json();

            // Defensive: check for txs array
            const txs = Array.isArray(blockInfo?.txs) ? blockInfo.txs : [];
            if (txs.length === 0) {
                console.log("No block info transactions");
                return;
            }

            const transactionPromises = txs.map(async (tx: any) => {
                // Defensive: Cosmos txs may have different structure, but we expect Ethermint EVM txs
                let txHash = '';
                let fromAddress = '';
                let toAddress = '';
                let value = '0';

                // Try to extract from Ethermint EVM tx structure
                try {
                    // Ethermint EVM txs are in tx.body.messages[0]
                    const msg = tx?.body?.messages?.[0];
                    // Only process if the message type does NOT include 'ethermint.evm'
                    if (msg && !(msg['@type'] && msg['@type'].includes('ethermint.evm'))) {
                        txHash = msg.hash || '';
                        fromAddress = msg.from || '';
                        // For legacy tx, to and value are in msg.data
                        if (msg.data && msg.data['@type'] && !msg.data['@type'].includes('ethermint.evm')) {
                            toAddress = msg.data.to || '';
                            value = msg.data.value || '0';
                        }
                    }
                } catch (err) {
                    console.error("Error extracting Ethermint tx fields", err);
                }

                const data: blocks_trnx = {
                    block_number: blockId,
                    tx_id: txHash,
                    value: value,
                    fromAddress: fromAddress,
                    toAddress: toAddress,
                    retry_count: 0,
                    key: null,
                    pair: null
                };

                data.key = (key === KEYS.latestBlock) ? 'latest' : 'behined & specific';
                const targetTopic = (key === KEYS.latestBlock) ? this.cosmos_block_tx_topic : this.cosmos_behind_block_tx_topic;

                try {
                    await kafkaHelper.produceMessage(targetTopic, data);
                } catch (err) {
                    console.error("Error while producing message for transaction", txHash, err);
                }
            });

            // Wait for all the transaction processing to complete
            await Promise.allSettled(transactionPromises);
        } catch (err: any) {
            console.error("Error in getTxFromBlock:", (err as Error).message);
            await global_helper.save_error_logs('cosmos_readBehindBlock', err.message)
        }
    }
    public async readBehindBlock() {
        try {
            // REDIS BASED
            let data_exist: any = await cosmosProcessHelper.getKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_COSMOS)
            if (data_exist) {
                let data_exists = JSON.parse(data_exist);
                if (data_exists) {
                    console.log("This block exist in redis read behined")
                    let { id, start_block, end_block } = data_exists;
                    if (end_block > start_block) {
                        console.log("This block go for getTxFromBlock")
                        await this.getTxFromBlock(start_block, KEYS.behinedBlock);
                        start_block = start_block + 1;
                        let add_data = {
                            id: id,
                            start_block: start_block,
                            end_block: end_block,
                            diff: end_block - start_block
                        }
                        await cosmosProcessHelper.setKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_COSMOS, JSON.stringify(add_data))
                    } else {
                        console.log("id>>>>>>>>>>>", id)
                        await TicsNativeOldBlockModel.update({ status: 1 }, { where: { id: id } })
                        await cosmosProcessHelper.deleteKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_COSMOS)

                        await cosmosProcessHelper.behindBlocks()

                    }
                }
            } else {
                // DB BASED
                await cosmosProcessHelper.behindBlocks()
            }
        } catch (err: any) {
            console.error("Error in readBehindBlock>>", err)
            await global_helper.save_error_logs('cosmos_readBehindBlock', err.message)
        }
    }
    public async readSpecificBlock() {
        try {
            console.log("Entere into readSpecificBlock>>>")
            let specificBlocks: any = await cosmosProcessHelper.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_COSMOS)
            let specific_block: any = JSON.parse(specificBlocks);
            console.log("🚀 ~ CosmosBlockProcess ~ readSpecificBlock ~ specific_block:", specific_block)
            if (specific_block && Number(specific_block) > 0) {
                console.table({ SPECIFIC_BLOCK: specific_block })
                await this.getTxFromBlock(specific_block, KEYS.readSpecificBlock)
                await cosmosProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_COSMOS, '0')
            } else {
                console.log("Specific block is not present>>>>")
                await cosmosProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_COSMOS, '0')
            }
        } catch (error: any) {
            console.error("Error in readSpecificBlock>>>", error)
        }
    }
    // public async testingNode() {
    //     try {
    //         const provider = Blockchain_Helper.provider;

    //         // Get Block Number
    //         const latestBlock = await provider.getBlockNumber();
    //         console.log("Block number>>", latestBlock);

    //         const blockInfo = await provider.getBlock(latestBlock, true);
    //         console.log("blockInfo>>", blockInfo);

    //         if (blockInfo && blockInfo.transactions.length > 0) {
    //             let firstTxHash: string;
    //             const firstTx = blockInfo.transactions[0];

    //             if (typeof firstTx === 'string') {
    //                 firstTxHash = firstTx;
    //             } else {
    //                 // Assert the type to access the hash property safely
    //                 firstTxHash = (firstTx as ethers.TransactionResponse).hash;
    //             }
    //             // Get Block Receipt
    //             const txReceipt = await provider.getTransactionReceipt(firstTxHash);
    //             console.log("txreceipt>>", txReceipt);

    //             // Get more details (not reduced in ethers.js)
    //             const transaction = await provider.getTransaction(firstTxHash);
    //             console.log("transaction>>", transaction);
    //         } else {
    //             console.log("No transactions found in the block");
    //         }
    //     } catch (err: any) {
    //         console.error("Error in testingNode>>", err)
    //     }
    // }
}

export const cosmosBlocksProcess = new CosmosBlockProcess();