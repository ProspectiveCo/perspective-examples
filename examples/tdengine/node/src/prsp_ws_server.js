import perspective from "@finos/perspective";
import * as taos from "@tdengine/websocket";
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// TDengine configuration
const TAOS_CONNECTION_URL = 'ws://localhost:6041';
const TAOS_USER = 'root';
const TAOS_PASSWORD = 'taosdata';
const TAOS_DATABASE = 'power';
const TAOS_TABLENAME = 'meters';

// Perspective configuration
const PERSPECTIVE_TABLE_NAME = 'meters';

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
                ts: new Date(Number(row[0])),
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
 * Create a Perspective table and host it via WebSocket.
 */
async function createPerspectiveServer(data) {
    const schema = {
        ts: "datetime",
        current: "float",
        voltage: "float",
        phase: "string",
        location: "string",
        groupid: "integer",
    };

    // Start a WebSocket server on port 8080
    const ws = new perspective.WebSocketServer({ port: 8080 });
    const table = await perspective.table(schema, { name: PERSPECTIVE_TABLE_NAME, limit: 1000, format: "json" });
    await table.update(data);

    console.log(`Perspective WebSocket server is running on ws://localhost:8080`);
}

/**
 * Main function to orchestrate the workflow.
 */
async function main() {
    const conn = await taosCreateConnection();
    await conn.exec(`USE ${TAOS_DATABASE};`);
    const data = await taosQuery(conn);
    console.log(data.slice(0, 2));
    await createPerspectiveServer(data);
    // await conn.close();
    // await taos.destroy();
}

main();
