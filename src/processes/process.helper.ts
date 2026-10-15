import { CoinInterface, TicsNativeAddressModel, TicsNativeOldBlockModel, WalletInterface, WalletModel } from "../models";
import redisClient from "../config/redis";
import { config } from "../config/config";
import { globalHelper } from "../helpers/global";
import { blockchainHelper } from "../helpers";

class CosmosProcessHelper {
  public CoinsInterface: CoinInterface[];
  public WalletAddresses: WalletInterface[];
  /**
   * Safely returns the uppercase version of a string, or an empty string if input is null/undefined.
   * @param {string | undefined | null} value
   * @returns {string}
   */
  private safeToUpperCase(value: string | undefined | null): string {
    if (typeof value === "string") {
      return value.toUpperCase();
    }
    return "";
  }

  constructor() {
    this.fetchWalletAddress();
  }


  public fetchWalletAddress = async () => {
    try {
      const addressData: any = await TicsNativeAddressModel.findAll({
        attributes: ["wallet_id", "user_id", "address"],
        include: [{
          model: WalletModel,
          as: 'wallet',
          attributes: ["wallet_name"],
          where: { status: 1, coin_family: config.STATIC_COIN_FAMILY.TICS }
        }],
        raw: true,
      });

      for await (const walletAddress of addressData) {

        const findWalletData = await this.getKeyValuePair(
          `${config.COSMOS_WALLET_ADDRESS}`,
          walletAddress.address.toUpperCase()
        );

        if (!findWalletData) {
          const walletRedisData = {
            address: walletAddress.address,
            user_id: walletAddress.user_id,
            wallet_name: walletAddress["wallet.wallet_name"],
            wallet_id: walletAddress.wallet_id
          }
          this.setKeyValuePair(
            `${config.COSMOS_WALLET_ADDRESS}`,
            walletAddress.address.toUpperCase(),
            JSON.stringify(walletRedisData)
          );
        }
      }
    } catch (err: any) {
      console.error('fetchWalletAddress >>>>>', err);
    }
  };
  public async getKeyValuePair(keyName: string, field: string) {
    try {
      const value = await redisClient.hGet(keyName, this.safeToUpperCase(field) || "");
      return value;
    } catch (err: any) {
      console.error('getKeyValuePair >>>>', err);
      return null;
    }
  }
  public async setBlocks(field: string, key: string, minBlockConfirmation: number, newBlock: number | null) {
    try {
      // Use node-fetch for HTTP requests since fetch is not defined in Node.js by default
      // Fetch latest Cosmos block from REST API
      const response = await blockchainHelper.fetchFromCosmosApi(config.NODE.COSMOS_SWAGGER_URL, {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
        },
        params: {
          path: "/cosmos/base/tendermint/v1beta1/blocks/latest",
        },
      });

      if (!response?.ok) {
        throw new Error(`Failed to fetch latest Cosmos block: ${response?.statusText}`);
      }
      const data = await response?.json();
      let latestBlockStr = data?.block?.header?.height || data?.sdk_block?.header?.height;
      if (!latestBlockStr) {
        throw new Error("Could not find block height in Cosmos REST API response");
      }
      let latestBlock = Number(latestBlockStr);
      if (isNaN(latestBlock)) {
        throw new Error("Block height from Cosmos REST API is not a number");
      }
      latestBlock = latestBlock - minBlockConfirmation;
      if (newBlock !== null && newBlock !== undefined) {
        if (newBlock < latestBlock) {
          await cosmosProcessHelper.setKeyValuePair(field, key, latestBlock.toString());
        } else {
          console.log("No updation of blocks");
        }
      } else {
        await cosmosProcessHelper.setKeyValuePair(field, key, latestBlock.toString());
      }
    } catch (err: any) {
      console.error("Error in setBlocks (Cosmos)", err);
      await globalHelper.save_error_logs('COSMOS_setBlocks', err.message || String(err));
    }
  }

  public async setKeyValuePair(keyName: string, field: string, value: string) {
    try {
      await redisClient.hSet(keyName, this.safeToUpperCase(field), value);
    } catch (err: any) {
      console.error('setKeyValuePair >>>', err);
    }
  }

  public async diffBlocksAddInDb(current_block: number, old_block: number) {
    try {
      let value_exist: any = await this.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.NODE_ALERT_COSMOS)
      if (value_exist) {
        let setvalue: number = Number(value_exist) + 1;
        if (Number(value_exist) <= 100) {
          await this.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.NODE_ALERT_COSMOS, setvalue.toString())
          if (Number(value_exist) == 0 || Number(value_exist) == 5 || Number(value_exist) == 10 || Number(value_exist) == 20 || Number(value_exist) == 50 || Number(value_exist) == 75 || Number(value_exist) == 100) {
            let email_body = `<p>Cosmos Node Issue ${Number(value_exist)}</p><p>Current block running = ${current_block}.</p><p>Old Block = ${old_block}.</p>`;
            await globalHelper.send_mail(config.NODE_ISSUE_ALERT_EMAIL, email_body, `Cosmos Node Issue ${Number(value_exist)}`);
          } else {
            console.log("no email")
          }
        } else {
          console.log("Number(value_exist) > 100")
        }
      } else {
        await this.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.NODE_ALERT_COSMOS, '0')
      }
      let dataToInsert = { block_number: old_block, coin_family: config.STATIC_COIN_FAMILY.TICS, start_block: old_block, end_block: current_block, status: 0 }
      console.log("dataToInsert>>>>", dataToInsert)
      await TicsNativeOldBlockModel.create(dataToInsert)
      return true;
    } catch (err: any) {
      console.error("💥 ~ Error in diff_blocks_add_in_redis.", err)
      await globalHelper.save_error_logs('COSMOS_diff_blocks_add_in_db', err.message)
      return false;
    }
  }

  public async deleteKeyValuePair(keyName: string, field: string) {
    try {
      await redisClient.hDel(keyName, this.safeToUpperCase(field));
      return true;
    } catch (err: any) {
      console.error("Error in deleteKeyValuePair>>>", err)
      return false;
    }
  }

  public checkIfContract = async (address: string) => {
    try {
      let token_data: any = await this.getKeyValuePair(config.TOKEN_TYPE_COSMOS, address);
      return JSON.parse(token_data);
    } catch (err: any) {
      console.error('checkOurWalletAddress >>>>>', err);
      return null;
    }
  };
  public async getMultipleKeyValuePairs(keyName: string, fields: string[]) {
    try {
      const upperFields = fields.map(f => f.toUpperCase());
      return await redisClient.hmGet(keyName, upperFields);
    } catch (err: any) {
      console.error('getMultipleKeyValuePairs >>>>', err);
      return null;
    }
  }

  public checkOurWalletAddress = async (address: string) => {
    try {
      let wallet_data: any = await this.getKeyValuePair(config.COSMOS_WALLET_ADDRESS, address);
      if (wallet_data) {
        console.log("wallet_Data>>>>>>>>", wallet_data)
      }
      return JSON.parse(wallet_data);
    } catch (error: any) {
      console.error('checkOurWalletAddress >>>>>', error);
      return null;
    }
  };

  public behindBlocks = async () => {
    try {
      let behindBlocks: any = await TicsNativeOldBlockModel.findOne({
        attributes: ['id', 'block_number', 'start_block', 'end_block'],
        where: { status: 0 },
        raw: true,
        order: [['id', 'ASC']]
      })
      if (behindBlocks) {
        let add_data = {
          id: behindBlocks.id,
          start_block: behindBlocks.start_block,
          end_block: behindBlocks.end_block,
          diff: behindBlocks.end_block - behindBlocks.start_block
        }
        await this.setKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_COSMOS, JSON.stringify(add_data))
      } else {
        console.log("you have not any behind block yet")
      }
    } catch (err: any) {
      console.error(`behindBlocks >>>`, err);
      return null;
    }
  }
}
const cosmosProcessHelper = new CosmosProcessHelper();
export default cosmosProcessHelper;