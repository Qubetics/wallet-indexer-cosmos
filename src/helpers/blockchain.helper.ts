import { Utility_Helper } from "./utility.helper";
import { ethers } from 'ethers';
import fetch from 'node-fetch';

class BlockchainHelper {
    constructor() {
      this.initialize();
    }
  
    public initialize() {}


    convertToWei = (value: number | string | bigint): string => {
        try {
            return ethers.parseUnits(value.toString(), 18).toString();
        } catch (error) {
            console.error(`convertToWei error >>`, error);
            return "0";
        }
    }
      
    // Fetch_Balance = async (
    //     address: string,
    //     coin: any
    // ) => {
    //     try {
    //         let balance: number | null = null;
    //         if (coin.is_token == BooleanEnum.true) {
    //             switch (coin.token_type) {

    //                 case TokenStandard.ERC20:
    //                     balance = await this.ERC20_Token_Fetch_Balance(
    //                         address, //// wallet_address
    //                         coin.token_address ? coin.token_address : null/// contract_address
    //                     )
    //                     break;
    //                 case TokenStandard.ERC721:
    //                     balance = await this.ERC721_Token_Fetch_Balance(
    //                         address, //// wallet_address
    //                         coin.token_address ? coin.token_address : null /// contract_address
    //                     )
    //                     break;
    //                 default:
    //                     balance = 0
    //                     break;
    //             }
    //         } else {
    //             balance = await this.Coin_Fetch_Balance(address);
    //         }
    //         return balance;
    //     } catch (error: any) {
    //         console.error(`Eth_Helper BSC_Fetch_Balance error >>`, error);
    //         return null;
    //     }
    // }

    // public ERC20_Token_Fetch_Balance = async (
    //     wallet_address: string,
    //     contract_address: string | null
    // ) => {
    //     try {
    //         if (contract_address) {
    //             const contract = new ethers.Contract(
    //               contract_address,
    //               config.config.CONTRACT_ABI,
    //               this.provider
    //             );
    //             const decimals = Number(await contract.decimals())
    //             const balance = await contract.balanceOf(wallet_address);
    //             const tokenBalance = await Utility_Helper.bigNumberSafeMath(
    //               balance.toString(),
    //               "/",
    //               Math.pow(10, decimals)
    //             );
    //             const tokBalance: string = await Utility_Helper.exponentialToDecimal(tokenBalance)
    //             return Number(tokBalance);
    //           }
    //           return null;
    //     } catch (error) {
    //         console.error(`Eth_Helper ERC20_Token_Fetch_Balance error >>`, error);
    //         return null;
    //         // throw error;
    //     }
    // }

    // private ERC721_Token_Fetch_Balance = async (
    //     wallet_address: string,
    //     contract_address: string | null
    // ) => {
    //     try {
    //         if (contract_address) {
    //             const contract = new ethers.Contract(
    //             contract_address,
    //             config.config.ERC721ABI,
    //             this.provider
    //             );
    //             const balance = await contract.balanceOf(wallet_address);
    //             return Number(balance.toString());
    //         }
    //         return null;
    //     } catch (error) {
    //         console.error(`Eth_Helper ERC20_Token_Fetch_Balance error >>`, error);
    //         // throw error;
    //         return null;
    //     }
    // }

    public Coin_Fetch_Balance = async (
        address: string
    ) => {
        try {
            console.log("Fetching balance for address:", address);
            const url = `https://swagger-internal.qubetics.com/cosmos/bank/v1beta1/balances/${address}`;
            const response = await fetch(url);
            if (!response.ok) {
                throw new Error(`Failed to fetch balance from Cosmos API: ${response.statusText}`);
            }
            const data = await response.json();
            // Find the balance for denom 'tics'
            let balanceObj = null;
            if (Array.isArray(data.balances)) {
                balanceObj = data.balances.find((b: any) => b.denom === "tics");
            }
            if (balanceObj && balanceObj.amount) {
                // Cosmos tics has 18 decimals
                const tokenBalance = await Utility_Helper.bigNumberSafeMath(
                    balanceObj.amount,
                    "/",
                    Math.pow(10, 18)
                );
                const coinBalance: string = await Utility_Helper.exponentialToDecimal(tokenBalance)
                console.log("Balance:", coinBalance);
                return Number(coinBalance);
            }
            return 0;
        } catch (error: any) {
            console.error(`Cosmos_Helper Coin_Fetch_Balance error >>`, error);
            return null;
        }
    }



}
export let Blockchain_Helper = new BlockchainHelper();