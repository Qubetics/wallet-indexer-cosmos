import { CoinInterface, TicsNativeOldBlockModel, WalletInterface } from "../models";
import redisClient from "../config/redis";
import { promisify } from "util";
import { config } from "../config/config";
import { global_helper } from "../helpers/global_helper";
import fetch from 'node-fetch';

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
    // this.fetchERC20Tokens();
    // this.fetchERC721Tokens();
    // this.fetchWalletAddress();
  }
  public async getKeyValuePair(keyName: string, field: string) {
    try {
      const getAsync = promisify(redisClient.HGET).bind(redisClient);
      const value = await getAsync(keyName, this.safeToUpperCase(field) || "");
      return value;
    } catch (err: any) {
      console.error('getKeyValuePair >>>>', err);
      return null;
    }
  }
  public async setBlocks(field: string, key: string, minBlockConfirmation: number, newBlock: number | null) {
    try {
      console.log("🚀 ~ CosmosProcessHelper ~ setBlocks ~ field:")
      // Use node-fetch for HTTP requests since fetch is not defined in Node.js by default
      // Fetch latest Cosmos block from REST API
      const response = await fetch("https://swagger-internal.qubetics.com/cosmos/base/tendermint/v1beta1/blocks/latest");
      if (!response.ok) {
        throw new Error(`Failed to fetch latest Cosmos block: ${response.statusText}`);
      }
      const data = await response.json();
      console.log("🚀 ~ CosmosProcessHelper ~ setBlocks ~ data:", data)
      let latestBlockStr = data?.block?.header?.height || data?.sdk_block?.header?.height;
      if (!latestBlockStr) {
        throw new Error("Could not find block height in Cosmos REST API response");
      }
      let latestBlock = Number(latestBlockStr);
      console.log("🚀 ~ CosmosProcessHelper ~ setBlocks ~ latestBlock:", latestBlock)
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
      await global_helper.save_error_logs('COSMOS_setBlocks', err.message || String(err));
    }
  }
  public async setKeyValuePair(keyName: string, field: string, value: string) {
    try {
      redisClient.hset(keyName, this.safeToUpperCase(field), value);
    } catch (err: any) {
      console.error('setKeyValuePair >>>', err);
    }
  }
  public async diff_blocks_add_in_db(current_block: number, old_block: number) {
    try {
      let value_exist: any = await this.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.NODE_ALERT_COSMOS)
      if (value_exist) {
        let setvalue: number = Number(value_exist) + 1;
        if (Number(value_exist) <= 100) {
          await this.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.NODE_ALERT_COSMOS, setvalue.toString())
          if (Number(value_exist) == 0 || Number(value_exist) == 5 || Number(value_exist) == 10 || Number(value_exist) == 20 || Number(value_exist) == 50 || Number(value_exist) == 75 || Number(value_exist) == 100) {
            let email_body = `<p>Cosmos Node Issue ${Number(value_exist)}</p><p>Current block running = ${current_block}.</p><p>Old Block = ${old_block}.</p>`;
            await global_helper.send_mail(config.NODE_ISSUE_ALERT_EMAIL, email_body, `Cosmos Node Issue ${Number(value_exist)}`);
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
      await global_helper.save_error_logs('COSMOS_diff_blocks_add_in_db', err.message)
      return false;
    }
  }

  public async getHashTable(keyName: string) {
    try {
      const getAsync: any = promisify(redisClient.hvals).bind(redisClient)
      const value: any = await getAsync(keyName)
      return value;
    } catch (err: any) {
      console.error("Error in getHashTable>>>", err)
      return null;
    }
  }
  public async deleteKeyValuePair(keyName: string, field: string) {
    try {
      redisClient.hdel(keyName, this.safeToUpperCase(field), function (err) {
        if (err) {
          throw err;
        }
        return true;
      })
    } catch (err: any) {
      console.error("Error in deleteKeyValuePair>>>", err)
      return false;
    }
  }



  // public fetchERC20Tokens = async () => {
  //   try {
  //     let tokens: any = await CoinsModel.findAll({
  //       attributes: ["coin_id", "coin_symbol", "coin_family", "is_token", "token_type", "decimals", "token_address", "for_swap", "added_by"],
  //       where: {
  //         coin_status: 1,
  //         coin_family: config.STATIC_COIN_FAMILY.ETH,
  //         [Op.or]: [{ token_type: TokenStandard.ERC20 }, { token_type: null }]
  //       },
  //       raw: true,
  //       logging: true
  //     });
  //     console.log('`${config.TOKEN_TYPE_ETH}`', `${config.TOKEN_TYPE_ETH}`)
  //     for await (let token of tokens) {
  //       let findTokenData = await this.getKeyValuePair(
  //         `${config.TOKEN_TYPE_ETH}`,
  //         token.token_address == null ? 'NativeCoin'.toUpperCase() : token.token_address.toUpperCase(),
  //       );
  //       if (!findTokenData) {
  //         this.setKeyValuePair(
  //           `${config.TOKEN_TYPE_ETH}`,
  //           token.token_address == null ? 'NativeCoin'.toUpperCase() : token.token_address.toUpperCase(),
  //           // token.token_address.toUpperCase(),
  //           JSON.stringify(token)
  //         );
  //       }
  //     }

  //     // redisClient.set(config.TOKEN_TYPE_ETH || "", JSON.stringify(tokens));
  //   } catch (err: any) {
  //     console.error('fetchERC20Tokens >>>>>', err);
  //     await global_helper.save_error_logs('ETH_fetchERC20Tokens', err.message)
  //   }
  // }

  // public fetchWalletAddress = async () => {
  //   try {
  //     const getaddressQuery = `SELECT ws.user_id,ws.wallet_address as address, ws.wallet_name FROM wallets ws FORCE INDEX (coin_family) JOIN coins cs ON ws.coin_id = cs.coin_id WHERE cs.coin_family = ${config.STATIC_COIN_FAMILY.TICS} GROUP BY ws.wallet_address`;
  //     const addresses: any = await database.db_write.query(
  //       getaddressQuery,
  //       { type: QueryTypes.SELECT }
  //     );
  //     for await (let wallet of addresses) {
  //       let findAddress = await this.getKeyValuePair(
  //         `${config.COSMOS_WALLET_ADDRESS}`,
  //         wallet.address.toUpperCase()
  //       );
  //       if (!findAddress) {
  //         this.setKeyValuePair(
  //           `${config.COSMOS_WALLET_ADDRESS}`,
  //           wallet.address.toUpperCase(),
  //           JSON.stringify(wallet)
  //         );
  //       }
  //     }
  //   } catch (error) {
  //     console.error('💥 ~ fetchWalletAddress error', error);
  //   }
  // };

  public checkIfContract = async (address: string) => {
    try {
      let token_data: any = await this.getKeyValuePair(config.TOKEN_TYPE_COSMOS, address);
      // if (token_data) {
      //   console.log("token_data>>>>>>>>", token_data)
      // }
      return JSON.parse(token_data);
    } catch (err: any) {
      console.error('check_our_wallet_address >>>>>', err);
      return null;
    }
  };

  // public checkIfContract = async (address: string) => {
  //   try {
  //     const getAsync = promisify(redisClient.get).bind(redisClient);
  //     const value = await getAsync(config.TOKEN_TYPE_ETH || "");

  //     const erc20Tokens: CoinInterface[] = value ? JSON.parse(value) : [];
  //     const token = erc20Tokens.find(
  //       (el: any) => Number(el.is_token) === Number(BooleanEnum.true) && el.token_address?.toUpperCase() === address?.toUpperCase()
  //     );
  //     if (token) return token;
  //     return null;
  //   } catch (error: any) {
  //     console.error('checkIfContract >>>', error);
  //     return null;
  //   }
  // };

  // public fetchERC721Tokens = async () => {
  //   try {
  //     const getTokenQuery = `SELECT coin_id,coin_symbol,coin_family,is_token, token_type,decimals,token_address,for_swap, added_by FROM coins WHERE coin_status=1 AND coin_family=${config.STATIC_COIN_FAMILY.ETH} AND token_type!='${TokenStandard.ERC20}' AND token_type IS NOT NULL`;

  //     const tokens: CoinInterface[] = await database.db_read.query(getTokenQuery, {
  //       type: Sequelize.QueryTypes.SELECT,
  //     });
  //     if (tokens.length > 0) {
  //       for await (let token of tokens) {
  //         if (token.token_address) {
  //           this.setKeyValuePair(
  //             `${config.COSMOS_NFT}`,
  //             token.token_address.toUpperCase(),
  //             JSON.stringify(token)
  //           );
  //         }
  //       }
  //     }
  //   } catch (error: any) {
  //     console.error('fetchERC721Tokens >>>>', error);
  //   }
  // };

  // public checkIfOurNFTContract = async (address: string) => {
  //   try {
  //     const token = await this.getKeyValuePair(`${config.COSMOS_NFT}`, address);
  //     if (token) {
  //       return JSON.parse(token);
  //     } else {
  //       return {}
  //     }
  //   } catch (error) {
  //     console.error(`💥 ~ checkIfContract error`, error);
  //   }
  // };

  public check_our_wallet_address = async (address: string) => {
    try {
      let wallet_data: any = await this.getKeyValuePair(config.COSMOS_WALLET_ADDRESS, address);
      if (wallet_data) {
        console.log("wallet_Data>>>>>>>>", wallet_data)
      }
      return JSON.parse(wallet_data);
    } catch (error: any) {
      console.error('check_our_wallet_address >>>>>', error);
      return null;
    }
  };
  // public NativeCoinByCoinFamily = async (
  //   coin_family: CoinFamilyEnum
  // ) => {
  //   try {
  //     const getAsync = promisify(redisClient.get).bind(redisClient);
  //     const value = await getAsync(config.TOKEN_TYPE_ETH || "");

  //     const erc20Tokens: CoinInterface[] = value ? JSON.parse(value) : [];
  //     return erc20Tokens.find(
  //       (el: CoinInterface) => Number(el.is_token) === Number(BooleanEnum.false) && el.coin_family === coin_family
  //     );

  //   } catch (error: any) {
  //     console.error(`NativeCoinByCoinFamily >>>`, error);
  //     return 0;
  //   }
  // }

  // public CoinByCoinId = async (
  //   coin_id: number
  // ) => {
  //   try {
  //     const getAsync = promisify(redisClient.get).bind(redisClient);
  //     const value = await getAsync(config.TOKEN_TYPE_ETH || "");

  //     const erc20Tokens: CoinInterface[] = value ? JSON.parse(value) : [];
  //     return erc20Tokens.find(
  //       (el: CoinInterface) => el.coin_id === coin_id
  //     );

  //   } catch (error: any) {
  //     console.error(`CoinByCoinId >>>`, error);
  //     return null;
  //   }
  // }
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