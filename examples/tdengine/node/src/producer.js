const taos = require("@tdengine/websocket");

// TDengine connection information
const TAOS_CONNECTION_URL = 'ws://localhost:6041';
const TAOS_USER = 'root';
const TAOS_PASSWORD = 'taosdata';
const TAOS_DATABASE = 'power';
const TAOS_TABLENAME = 'meters';

// Data generation constants
const MAX_RUN_DURATION = 10 * 1000;    // 1 hour in milliseconds
const INTERVAL = 250;                       // interval in milliseconds
const NUM_ROWS_PER_INTERVAL = 100;          // rows generated per interval
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


/**
 * Generates random power-line data: `ts`, `current`, `voltage`, and `phase`.
 */
function generateData(num_rows = NUM_ROWS_PER_INTERVAL) {
    const modifier = Math.random() * (Math.random() * 50 + 1);
    return Array.from({ length: num_rows }, (_, i) => ({
        ts: Date.now() + i,
        current: Math.random() * 75 + Math.random() * 10 * modifier,
        voltage: Math.floor(Math.random() * 26) + 200,
        phase: Math.random() * 105 + Math.random() * 3 * modifier,
    }));
}


/**
 * Connect to a TDengine database using WebSocket APIs.
 */
async function taosCreateConnection(
    url = TAOS_CONNECTION_URL, 
    user = TAOS_USER, 
    password = TAOS_PASSWORD
) {
    try {
        // create the connection configuration
        let conf = new taos.WSConfig(url);
        conf.setUser(user);
        conf.setPwd(password);
        // connect to the TDengine database
        conn = await taos.sqlConnect(conf);
        console.log(`Connected to ${url} successfully.`);
        return conn;
    } catch (err) {
        console.log(`Failed to connect to ${url}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        console.log("This is most likely due to the TDengine docker container not running or the connection information being incorrect... Please run `/docker.sh` to start the TDengine docker container.");
        process.exit(1);
    }
}


/**
 * Create a TDengine database and table.
 */
async function taosCreateDatabase(conn, databaseName = TAOS_DATABASE) {
    try {
        // create the database with a 1-year retention policy
        await conn.exec(`CREATE DATABASE IF NOT EXISTS ${databaseName} KEEP 365 DURATION 10 BUFFER 16 WAL_LEVEL 1;`);
        // switch to the database
        await conn.exec(`USE ${databaseName};`);
        console.log(`Database ${databaseName} created successfully.`);
    } catch (err) {
        console.error(`Failed to create database ${databaseName}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err;
    }
}


/**
 * Create a TDengine table.
 */
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


/**
 * Insert data into a TDengine table using the WebSocket API, subtables, and batch API for performance.
 */
async function toasInsertData(conn, data, databaseName = TAOS_DATABASE, tableName = TAOS_TABLENAME) {
    try {
        // pick a random subtable
        const subTableId = Math.floor(Math.random() * LOCATIONS.length);
        const groupId = subTableId;
        const locationName = LOCATIONS[subTableId];
        const subTableName = `d_meters_${subTableId}`;

        let stmt = await conn.stmtInit();
        await stmt.prepare(`INSERT INTO ? USING ${databaseName}.${tableName} tags(?,?) VALUES (?,?,?,?)`);
        await stmt.setTableName(subTableName);

        // set tags
        let tagParams = stmt.newStmtParam();
        tagParams.setVarchar([locationName]);
        tagParams.setInt([groupId]);
        await stmt.setTags(tagParams);
        // set data
        let bindParams = stmt.newStmtParam();
        bindParams.setTimestamp(data.map(row => row.ts));
        bindParams.setFloat(data.map(row => row.current));
        bindParams.setInt(data.map(row => row.voltage));
        bindParams.setFloat(data.map(row => row.phase));
        await stmt.bind(bindParams);
        await stmt.batch();
        await stmt.exec();
        console.log(`Inserted ${data.length} rows into -- table: ${TAOS_TABLENAME}, subtable: ${subTableName}, location: ${locationName}`);
    } catch (err) {
        console.error(`Failed to insert rows into ${TAOS_TABLENAME}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err
    }
}


/**
 * Main function to generate and insert random power line data into a TDengine database at regular intervals.
 * 
 * We establish a connection to the TDengine database, creates the necessary database and table,
 * and then starts a loop to generate and insert data at a specified interval. The loop runs until a maximum
 * duration is reached, at which point the connection is closed, and the process exits.
 * 
 * Workflow:
 * 1. Create a TDengine connection >> `taosCreateConnection()`.
 * 2. Create the demo database and table >> `taosCreateDatabase()`, `taosCreateTable()`.
 * 3. Start a data generation and insertion timer loop >> `generateData()`, `toasInsertData()`.
 * 
 */
async function main() {
    try {
        // create a connection to the TDengine database
        let conn = await taosCreateConnection();
        // create the database and table
        await taosCreateDatabase(conn);
        await taosCreateTable(conn);
        await conn.exec(`USE ${TAOS_DATABASE};`);

        // start the data generation and insertion loop
        const startTime = Date.now();
        console.log("Starting data generation and insertion loop...");

        // insert data at a regular interval -- 250ms default
        const intervalId = setInterval(async () => {
            try {
                // exit if the max run duration is reached -- 1 hour default
                if ((Date.now() - startTime) >= MAX_RUN_DURATION) {
                    console.log("Max run duration reached. Exiting...");
                    clearInterval(intervalId);
                    await conn.close();
                    taos.destroy();
                    process.exit(0);
                }
                // generate and insert data
                let data = generateData();
                await toasInsertData(conn, data);
            } catch (err) {
                console.error("Error during data insertion:", err);
            }
        }, INTERVAL);
    } catch (err) {
        console.log("Demo Failed! ErrCode: " + err.code + ", ErrMessage: " + err.message);
    }
}


// Run the main function
main();
