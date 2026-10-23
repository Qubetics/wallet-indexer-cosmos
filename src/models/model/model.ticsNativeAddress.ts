import { DataTypes, Model, Optional } from "sequelize";
import db from "../../config/db";
import WalletModel from "./model.wallets";
import { TicsNativeInterface } from "../interfaces/index";

// Optional ID for creation
interface TicsNativeCreationAttrs extends Optional<TicsNativeInterface, "id"> {}

interface TicsNativeInstance
  extends Model<TicsNativeInterface, TicsNativeCreationAttrs>,
    TicsNativeInterface {}

const TicsNativeAddressModel = db.db_write.define<TicsNativeInstance>(
  "tics_native_addresses",
  {
    id: {
      type: DataTypes.INTEGER.UNSIGNED,
      autoIncrement: true,
      primaryKey: true,
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
    },
    address: {
      type: DataTypes.STRING(100),
      allowNull: false,
      unique: true,
    },
    created_at: {
      type: DataTypes.DATE,
      allowNull: false,
      defaultValue: DataTypes.NOW,
    },
    updated_at: {
      type: DataTypes.DATE,
      allowNull: false,
      defaultValue: DataTypes.NOW,
    },
  },
  {
    indexes: [
      {
        unique: true,
        fields: ["address"],
      },
      {
        fields: ["wallet_id"],
      },
    ],
    underscored: true,
    timestamps: false,
    tableName: "tics_native_addresses",
  }
);

// Associations
TicsNativeAddressModel.belongsTo(WalletModel, {
  foreignKey: "wallet_id",
  as: "wallet",
});

WalletModel.hasOne(TicsNativeAddressModel, {
  foreignKey: "wallet_id",
  as: "tics_native_addresses",
});


export default TicsNativeAddressModel;
