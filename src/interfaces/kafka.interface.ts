import { BlockChainStatusEnum, TrnxTypeEnum } from "../enum";

export interface KafkaCrednetials {
    clientId: string;
    brokers: string[];
}

export interface IChainAbsTxData {
    id: number;
    tx_id: string | null;
    type: TrnxTypeEnum;
    status: any;
    blockchain_status: BlockChainStatusEnum;
    created_at?: Date | null;
}