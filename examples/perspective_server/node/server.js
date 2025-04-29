import perspective from "@finos/perspective";

// --- Demo constants ---
const NUMBER_OF_ROWS = 100; // Number of rows to generate per interval
const INTERVAL = 250; // Interval in milliseconds

// --- Data generation ---
const PERSPECTIVE_TABLE_NAME = "stock_values"; // Name of the Perspective table
const SECURITIES = [
    "AAPL.N", "AMZN.N", "QQQ.N", "NVDA.N", "TSLA.N", "FB.N", "MSFT.N", "TLT.N", "XIV.N", "YY.N",
    "CSCO.N", "GOOGL.N", "PCLN.N", "NFLX.N", "BABA.N", "INTC.N", "V.N", "JPM.N", "WMT.N", "DIS.N",
    "PYPL.N", "ADBE.N", "CMCSA.N", "PEP.N", "KO.N", "NKE.N", "MRK.N", "PFE.N", "T.N", "VZ.N",
    "ORCL.N", "IBM.N", "CRM.N", "BA.N", "GE.N"
];
const CLIENTS = ["Homer", "Marge", "Bart", "Lisa", "Maggie", "Moe", "Lenny", "Carl", "Krusty"];

/**
 * Generate random stock data.
 */
function generateData(nrows = NUMBER_OF_ROWS) {
    const baseModifier = Math.random() * (50 - 1) + 1; // Random float between 1 and 50
    return Array.from({ length: nrows }, () => ({
        ticker: SECURITIES[Math.floor(Math.random() * SECURITIES.length)],
        client: CLIENTS[Math.floor(Math.random() * CLIENTS.length)],
        open: baseModifier * (Math.random() * (1.2 - 0.8) + 0.8),
        high: baseModifier * (Math.random() * (1.5 - 1.0) + 1.0),
        low: baseModifier * (Math.random() * (1.0 - 0.7) + 0.7),
        close: baseModifier * (Math.random() * (1.3 - 0.8) + 0.8),
        lastUpdate: new Date().toISOString(),
        date: new Date().toISOString().split("T")[0]
    }));
}

/**
 * Create a Perspective table.
 */
async function createPerspectiveTable() {
    const schema = {
        ticker: "string",
        client: "string",
        open: "float",
        high: "float",
        low: "float",
        close: "float",
        lastUpdate: "datetime",
        date: "date"
    };
    const table = await perspective.table(schema, {
        name: PERSPECTIVE_TABLE_NAME,
        limit: 2500,
        format: "json"
    });
    console.log(`Created Perspective table: '${PERSPECTIVE_TABLE_NAME}'`);
    return table;
}

/**
 * Main function to initialize and run the Perspective server.
 */
async function main() {
    // Create a Perspective WebSocket server
    const server = new perspective.WebSocketServer({ port: 8080 });
    console.log("Perspective WebSocket server is running on ws://localhost:8080/websocket");

    // Create the Perspective table
    const table = await createPerspectiveTable();
    let counter = 0;

    // Set up a timer to periodically generate data and update the table
    // !!! NOTE: It's important to use async/await here to ensure Node main event loop is not blocked
    //           and to handle any potential errors in the async function.
    const interval = setInterval(async () => {
        try {
            const data = generateData();
            await table.update(data);
            process.stdout.write((++counter % 120 === 0) ? ".\n" : ".");    // show progress
        } catch (err) {
            console.error("Error updating Perspective table:", err);
            clearInterval(interval);
        }
    }, INTERVAL);
}

// Run the main function
main().catch(err => {
    console.error("Error starting the server:", err);
    process.exit(1);
});
