import perspective from "@finos/perspective";
import * as taos from "@tdengine/websocket";


// TDengine configuration
const TAOS_CONNECTION_URL = 'ws://localhost:6041';
const TAOS_USER = 'root';
const TAOS_PASSWORD = 'taosdata';
const TAOS_DATABASE = 'power';
const TAOS_TABLENAME = 'meters';

// Perspective configuration
const PRSP_TABLE_NAME = TAOS_TABLENAME;
const PRSP_TABLE_LIMIT = 100_000;                  // Limit for the Perspective table nmber of rows
const PRSP_TABLE_REFRESH_INTERVAL = 250;           // Refresh interval in milliseconds


/**
 * Connect to a TDengine database using WebSocket APIs.
 */
async function taosCreateConnection(
    url = TAOS_CONNECTION_URL, 
    user = TAOS_USER, 
    password = TAOS_PASSWORD
) {
    try {
        let conf = new taos.WSConfig(url);
        conf.setUser(user);
        conf.setPwd(password);
        const conn = await taos.sqlConnect(conf);
        console.log(`Connected to ${url} successfully.`);
        return conn;
    } catch (err) {
        console.error(`Failed to connect to ${url}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        process.exit(1);
    }
}

/**
 * Query the TDengine meters table and return the result as an array of objects.
 */
async function taosQuery(conn, databaseName = TAOS_DATABASE, tableName = TAOS_TABLENAME) {
    try {
        const sql = `
            SELECT 
                ts, current, voltage, phase, location, groupid 
            FROM ${databaseName}.${tableName} 
            ORDER BY ts DESC;
        `;
        const wsRows = await conn.query(sql);
        const data = [];
        while (await wsRows.next()) {
            let row = wsRows.getData();
            data.push({
                ts: new Date(Number(row[0])),       // convert to timestamp
                current: row[1],
                voltage: row[2],
                phase: row[3],
                location: row[4],
                groupid: row[5],
            });
        }
        return data;
    } catch (err) {
        console.error(`Failed to query table ${databaseName}.${tableName}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err;
    }
}

/**
 * Create a Perspective table to host on the websocket.
 */
async function prspCreatePerspectiveTable() {
    // create an empty table with schema
    const schema = {
        ts: "datetime",
        current: "float",
        voltage: "float",
        phase: "string",
        location: "string",
        groupid: "integer",
    };
    // create a table with schema and row limit
    // other supported formats: "json", "columns", "csv" or "arrow", "ndjson"
    const table = await perspective.table(schema, { name: PRSP_TABLE_NAME, limit: PRSP_TABLE_LIMIT, format: "json" });
    return table;
}


/**
 * Main function to orchestrate the workflow.
 */
async function main() {
    // create tdengine connection and perspective websocket server
    const conn = await taosCreateConnection();
    const ws = new perspective.WebSocketServer({ port: 8080 });

    // create a perspective table
    const table = await prspCreatePerspectiveTable();

    console.log(`Perspective WebSocket server is running on ws://localhost:8080`);

    // Set up a timer to periodically query TDengine and update the Perspective table
    setInterval(async () => {
        try {
            const data = await taosQuery(conn);
            await table.update(data);
            // console.log(`Perspective table refreshed: ${data.length} rows.`);
        } catch (err) {
            console.error(`Error updating Perspective table: ${err.message}`);
            console.error(`Exiting...`);
            await table.clear();
            await conn.close();
            await taos.destroy();
            process.exit(1);
        }
    }, PRSP_TABLE_REFRESH_INTERVAL);
}


// Run the main function
main();
