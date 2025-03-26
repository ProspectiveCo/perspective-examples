const taos = require("@tdengine/websocket");

// TDengine connection information
const TAOS_CONNECTION_URL = 'ws://localhost:6041';
const TAOS_USER = 'root';
const TAOS_PASSWORD = 'taosdata';
const TAOS_DATABASE = 'power';
const TAOS_TABLENAME = 'meters';

// Data generation constants
const NUM_ROWS_PER_INTERVAL = 100;
const LOCATIONS = [
    "San Francisco", 
    "Los Angles", 
    "San Diego",
    "San Jose", 
    "Palo Alto", 
    "Campbell", 
    "Mountain View",
    "Sunnyvale", 
    "Santa Clara", 
    "Cupertino"
]


async function taosCreateConnection() {
    try {
        let conf = new taos.WSConfig(TAOS_CONNECTION_URL);
        conf.setUser(TAOS_USER);
        conf.setPwd(TAOS_PASSWORD);
        conn = await taos.sqlConnect(conf);
        console.log(`Connected to ${TAOS_CONNECTION_URL} successfully.`);
        return conn;
    } catch (err) {
        console.log(`Failed to connect to ${TAOS_CONNECTION_URL}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        console.log("This is most likely due to the TDengine docker container is not running or the connection information is incorrect... Please run `/docker.sh` to start the TDengine docker container.");
        process.exit(1);
    }
}

async function taosCreateDatabase(conn, databaseName = TAOS_DATABASE) {
    try {
        await conn.exec(`CREATE DATABASE IF NOT EXISTS ${databaseName} KEEP 3650 DURATION 10 BUFFER 16 WAL_LEVEL 1;`);
        await conn.exec(`USE ${databaseName};`);
        console.log(`Database ${databaseName} created successfully.`);
    } catch (err) {
        console.error(`Failed to create database ${databaseName}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err;
    }
}

async function taosCreateTable(conn, databaseName = TAOS_DATABASE, tableName = TAOS_TABLENAME) {
    try {
        // drop the table if it already exists
        await conn.exec(`DROP TABLE IF EXISTS ${databaseName}.${tableName}`);
        // create the table
        const sql = `
            CREATE STABLE IF NOT EXISTS ${databaseName}.${tableName} (
                ts TIMESTAMP, 
                current FLOAT, 
                voltage INT, 
                phase FLOAT
            ) TAGS (
                location BINARY(64),
                groupid INT
            );
        `;
        await conn.exec(sql);
        console.log(`TDengine - Created table ${tableName}`);
    } catch (err) {
        console.error(`Failed to create table ${tableName}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err;
    }
}

function generateData(num_rows = NUM_ROWS_PER_INTERVAL) {
    const data = [];
    const modifier = Math.random() * Math.floor(Math.random() * 50 + 1);
    for (let i = 0; i < num_rows; i++) {
        data.push({
            ts: new Date().getTime() + i,
            current: Math.random() * 75 + Math.floor(Math.random() * 10) * modifier,
            voltage: Math.floor(Math.random() * 26) + 200,
            phase: Math.random() * 105 + Math.floor(Math.random() * 3 + 1) * modifier,
        });
    }
    return data;
}

async function toasInsertData(conn, data) {
    try {
        // pick a random subtable
        const subTableId = Math.floor(Math.random() * LOCATIONS.length);
        const groupId = subTableId;
        const locationName = LOCATIONS[subTableId];

        let stmt = await conn.stmtInit();
        await stmt.prepare(`INSERT INTO ? USING ${TAOS_DATABASE}.${TAOS_TABLENAME} tags(?,?) VALUES (?,?,?,?)`);
        // await stmt.prepare(`INSERT INTO ${TAOS_TABLENAME} VALUES (?,?,?,?)`);
        await stmt.setTableName(`d_meters_${subTableId}`);

        // set tags
        let tagParams = stmt.newStmtParam();
        tagParams.setVarchar([locationName]);
        tagParams.setInt([groupId]);
        await stmt.setTags(tagParams);
        // set data
        let bindParams = stmt.newStmtParam();
        timestampParams = data.map(row => row.ts);
        currentParams = data.map(row => row.current);
        voltageParams = data.map(row => row.voltage);
        phaseParams = data.map(row => row.phase);
        console.log(`timestampParams: ${timestampParams.length}`);
        bindParams.setTimestamp(timestampParams);
        bindParams.setFloat(currentParams);
        bindParams.setInt(voltageParams);
        bindParams.setFloat(phaseParams);
        // bindParams.setTimestamp(data.map(row => row.ts));
        // bindParams.setFloat(data.map(row => row.current));
        // bindParams.setInt(data.map(row => row.voltage));
        // bindParams.setFloat(data.map(row => row.phase));
        await stmt.bind(bindParams);
        await stmt.batch();
        await stmt.exec();
        console.log(`Inserted ${data.length} rows into table ${TAOS_TABLENAME}`);
    } catch (err) {
        console.error(`Failed to insert rows into ${TAOS_TABLENAME}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err
    }
}


async function main() {
    try {
        let conn = await taosCreateConnection();
        await taosCreateDatabase(conn);
        await taosCreateTable(conn);
        await conn.exec(`USE ${TAOS_DATABASE}`);
        let data = generateData();
        await toasInsertData(conn, data);
        await conn.close();
        console.log("Demo completed.");
    } catch (err) {
        console.log("Demo Failed! ErrCode: " + err.code + ", ErrMessage: " + err.message);
    } finally {
        taos.destroy();
    }
}


// Run the main function
main();
