import { config } from "../config/config";
import cosmosProcessHelper from "./process.helper";
import { KEYS } from "../enum";
import { TicsNativeOldBlockModel } from "../models";
import { globalHelper } from "../helpers/global";
import kafkaHelper from "../config/kafka";
import { blockchainHelper } from "../helpers";
class ProcessedBlocks {
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
            if (old_block && new_block) {
                let oldBlock: number = Number(old_block);
                let newBlock = Number(new_block);
                if (newBlock <= oldBlock) {
                    await cosmosProcessHelper.setBlocks(config.REDISKEYS.COSMOS_BLOCKS, config.REDISKEYS.NEW_BLOCK_COSMOS, this.minBlockConfirmation, newBlock)
                } else {
                    let block_diff: number = newBlock - oldBlock;
                    if (block_diff > 50) {
                        await cosmosProcessHelper.diffBlocksAddInDb(newBlock, oldBlock)
                        await cosmosProcessHelper.setKeyValuePair(config.REDISKEYS.COSMOS_BLOCKS, config.REDISKEYS.OLD_BLOCK_COSMOS, newBlock.toString())
                        await cosmosProcessHelper.setBlocks(config.REDISKEYS.COSMOS_BLOCKS, config.REDISKEYS.NEW_BLOCK_COSMOS, this.minBlockConfirmation, newBlock)
                    } else {
                        await this.readBlocks(oldBlock, newBlock)
                    }
                }
            } else {
                console.log(" no old block and no new block")
                await globalHelper.save_error_logs('cosmos_setBlocks', 'No old block no new block')
            }
        } catch (err: any) {
            console.error("Error in getBlocks", err)
            await globalHelper.save_error_logs('cosmos_getBlocks', err.message)
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
            await globalHelper.save_error_logs('cosmos_readBlocks', err.message)
        }
    }

    public async getTxFromBlock(blockId: number, key: string) {
        try {
            // Fetch block transactions via Cosmos REST API
            const response = await blockchainHelper.fetchFromCosmosApi(config.NODE.COSMOS_SWAGGER_URL, {
                method: "GET",
                headers: { "Content-Type": "application/json" },
                params: { path: `/cosmos/tx/v1beta1/txs/block/${blockId}` },
            });

            if (!response?.ok) {
                console.log("No block info from Cosmos REST API");
                await TicsNativeOldBlockModel.create({
                    block_number: blockId,
                    coin_family: config.STATIC_COIN_FAMILY.TICS,
                    start_block: blockId,
                    end_block: blockId,
                    status: 0,
                });
                return;
            }

            const blockInfo = await response.json();

            const blockTxs = Array.isArray(blockInfo?.block?.data?.txs)
                ? blockInfo.block.data.txs
                : [];

            if (blockTxs.length === 0) {
                console.log("No transactions found in block");
                return;
            }


            //will revert this  
            const txs = Array.isArray(blockInfo?.txs) ? blockInfo.txs : [];


            const targetTopic = this.cosmos_block_tx_topic


            for (let i = 0; i < txs.length; i++) {
                const tx = txs[i];
                const msg = tx?.body?.messages?.[0];

                // Skip Ethermint EVM transactions
                if (msg?.["@type"]?.includes("ethermint.evm")) {
                    console.log(`Skipping EVM tx in block ${blockId}`);
                    continue;
                }

                // Prepare corresponding base64 tx (if exists)
                const base64Tx = blockTxs[i];
                if (!base64Tx) continue;

                const data = {
                    block_number: blockId,
                    tx_base64: base64Tx,
                    key: key === KEYS.latestBlock ? "latest" : "behind & specific",
                    pair: null,
                    retry_count: 0,
                };

                try {
                    await kafkaHelper.produceMessage(targetTopic, data);
                } catch (err) {
                    console.error(`Kafka produce error for block ${blockId}:`, err);
                }
            }
        } catch (err: any) {
            console.error("Error in getTxFromBlock:", err.message);
            await globalHelper.save_error_logs("cosmos_readBehindBlock", err.message);
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
            await globalHelper.save_error_logs('cosmos_readBehindBlock', err.message)
        }
    }
    public async readSpecificBlock() {
        try {
            console.log("Entere into readSpecificBlock>>>")
            let specificBlocks: any = await cosmosProcessHelper.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_COSMOS)
            let specific_block: any = JSON.parse(specificBlocks);
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
}

export const processedBlocks = new ProcessedBlocks();