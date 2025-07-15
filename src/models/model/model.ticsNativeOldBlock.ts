import { DataTypes, Model, Optional } from "sequelize";
import { TicsNativeOldBlockInterface } from "../interfaces/index";
import db from "../../config/db";

interface TicsNativeOldBlockCreationModel extends Optional<TicsNativeOldBlockInterface, "id"> { }
interface TicsNativeOldBlockInstance
    extends Model<TicsNativeOldBlockInterface, TicsNativeOldBlockCreationModel>,
    TicsNativeOldBlockInterface { }

let dataObj = {
    id: {
        primaryKey: true,
        type: DataTypes.INTEGER,
        allowNull: false,
        autoIncrement: true,
    },
    block_number: {
        type: DataTypes.INTEGER,
        allowNull: false,
    },
    start_block: {
        type: DataTypes.INTEGER,
        allowNull: false,
    },
    end_block: {
        type: DataTypes.INTEGER,
        allowNull: false,
    },
    coin_family: {
        type: DataTypes.INTEGER,
        allowNull: false,
    },
    status: {
        type: DataTypes.TINYINT,
        allowNull: true,
    }
};
let dataObjIndex = {
    createdAt: 'created_at',
    updatedAt: 'updated_at',
    indexes: [
        {
            unique: false,
            fields: ["status"],
        }
    ]
}


const TicsNativeOldBlockModel = db.db_write.define<TicsNativeOldBlockInstance>("tics_native_old_blocks", dataObj, dataObjIndex);

export default TicsNativeOldBlockModel;