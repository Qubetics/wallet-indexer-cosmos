import { DataTypes, Model, Optional } from "sequelize";
import { DvpnSessionsInterface } from "../interfaces/interface.dvpn_sessions";
import db from "../../config/db";

interface DvpnSessionsModel extends Optional<DvpnSessionsInterface, "id"> { }
interface DvpnSessionsInstance
    extends Model<DvpnSessionsInterface, DvpnSessionsModel>,
    DvpnSessionsInterface { }

let dataObj: any = {
    id: {
        type: DataTypes.INTEGER,
        allowNull: false,
        primaryKey: true,
        autoIncrement: true,
    },
    wallet_id: {
        type: DataTypes.INTEGER,
        allowNull: false,
        references: {
          model: "wallets",
          key: "wallet_id",
        },
        onUpdate: "CASCADE",
        onDelete: "CASCADE",
      },
    user_id: {
        type: DataTypes.INTEGER,
        allowNull: false,
        references: {
            model: "users",
            key: "user_id",
          },
          onUpdate: "CASCADE",
          onDelete: "CASCADE",
      },
    status: {
        type: DataTypes.STRING,
        allowNull: false,
        Comment: 'session status',
    },
    block: {
        type: DataTypes.INTEGER,
        allowNull: true,
        Comment: 'Block number',
    },
    tx_hash: {
        type: DataTypes.STRING,
        allowNull: true,
        Comment: 'tx hash',
    },
    node_address: {
        type: DataTypes.STRING,
        allowNull: true,
        defaultValue: null,
        Comment: 'dvpn node address',
    },
    node_id: {
        type: DataTypes.STRING,
        allowNull: true,
        defaultValue: null,
        Comment: 'dvpn node id',
    },
    event: {
        type: DataTypes.STRING,
        allowNull: true,
        defaultValue: null,
        Comment: 'dvpn event',
    },
    status_at: {
        type: DataTypes.STRING,
        allowNull: true,
        defaultValue: null,
        Comment: 'session status at',
    },
    start_at: {
        type: DataTypes.STRING,
        allowNull: true,
        defaultValue: null,
        Comment: 'session start at',
    },
    extra_data: {
        type: DataTypes.JSON,
        allowNull: true,
        defaultValue: null,
        Comment: 'extra data',
    },
};

let dataObjIndex = {
    indexes: [
      {
        unique: false,
        fields: ["user_id"],
      }
    ],
  };

const DvpnSessionsModel = db.db_write.define<DvpnSessionsInstance>("dvpn_sessions", dataObj, dataObjIndex);

DvpnSessionsModel.sync({alter : true})

export default DvpnSessionsModel;