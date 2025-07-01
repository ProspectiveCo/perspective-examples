import { connect, deferred, nanos } from "nats.ws";
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import fetch from 'node-fetch';
import { asyncBufferFromFile, parquetReadObjects } from 'hyparquet';

// Get current directory for ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// NATS configuration
const NATS_SERVER_URL = "ws://localhost:8080";

// Data configuration
const RUN_DURATION          = 30 * 60 * 1000;   // 30 minutes
const DEFAULT_CHUNK_SIZE    = 10;               // Default number of rows per chunk
const DEFAULT_INTERVAL      = 300;              // Default interval in milliseconds
const BLOTTER_DATA_URL      = "https://perspective-demo-dataset.s3.us-east-1.amazonaws.com/pro_capital_markets/blotter_data_30yrs.parquet";
const LOCAL_BLOTTER_FILE    = path.join(__dirname, "blotter_data_30yrs.parquet");

/* -------------------------------------------------------------
 * Data fetching and processing functions
 ----------------------------------------------------------------
 */

/**
 * Downloads the parquet file if it doesn't exist locally
 */
async function fetchBlotterDataFile() {
    if (fs.existsSync(LOCAL_BLOTTER_FILE)) {
        console.log("Parquet file already exists locally, skipping download.");
        return;
    }

    console.log("Downloading parquet file...");
    try {
        const response = await fetch(BLOTTER_DATA_URL);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const buffer = await response.arrayBuffer();
        fs.writeFileSync(LOCAL_BLOTTER_FILE, Buffer.from(buffer));
        console.log(`Parquet file downloaded: ${LOCAL_BLOTTER_FILE}`);
    } catch (error) {
        console.error("Error downloading parquet file:", error);
        throw error;
    }
}

/**
 * Parquet data reader for blotter data using hyparquet
 */
class BlotterDataReader {
    constructor(filePath) {
        this.filePath = filePath;
        this.currentIndex = 0;
        this.data = [];
        this.isLoaded = false;
    }

    /**
     * Load data from parquet file using hyparquet
     */
    async loadData() {
        if (this.isLoaded) {
            return;
        }

        try {
            console.log("Loading parquet file with hyparquet...");
            
            // Create an AsyncBuffer from the file
            const file = await asyncBufferFromFile(this.filePath);
            
            // Read the parquet file as objects
            const data = await parquetReadObjects({ file });
            
            console.log(`Successfully loaded ${data.length} records from parquet file`);
            
            // Convert the data to match our expected format
            this.data = data.map(record => {
                const converted = {};
                for (const [key, value] of Object.entries(record)) {
                    // Handle different data types properly
                    if (value instanceof Date) {
                        converted[key] = value.toISOString();
                    } else if (typeof value === 'bigint') {
                        converted[key] = Number(value);
                    } else {
                        converted[key] = value;
                    }
                }
                return converted;
            });
            
            this.isLoaded = true;
            console.log(`Successfully processed ${this.data.length} records`);
            
            // Log sample record for verification
            if (this.data.length > 0) {
                console.log("Sample record schema:", Object.keys(this.data[0]));
                console.log("First record:", JSON.stringify(this.data[0], null, 2));
            }
            
        } catch (error) {
            console.error("Error loading parquet file:", error);
            console.log("Falling back to mock data generation...");
            this.data = this.generateMockBlotterData(1000);
            this.isLoaded = true;
        }
    }

    /**
     * Generate mock blotter data that matches the schema from blotter.py
     * (Fallback if parquet file cannot be read)
     */
    generateMockBlotterData(numRecords) {
        const data = [];
        
        // Mock data constants
        const symbols = ['AAPL.N', 'MSFT.N', 'GOOGL.N', 'AMZN.N', 'TSLA.N', 'NVDA.N', 'META.N', 'NFLX.N'];
        const traders = ['Alice_Chen', 'Bob_Johnson', 'Carol_Smith', 'David_Wilson', 'Eva_Rodriguez', 'Frank_Kim'];
        const desks = ['Equity_Desk', 'Options_Desk', 'Fixed_Income', 'Derivatives'];
        const sides = ['BUY', 'SELL'];
        const orderTypes = ['LIMIT', 'MARKET', 'STOP', 'STOP_LIMIT'];
        const orderStatuses = ['NEW', 'PARTIALLY_FILLED', 'FILLED', 'CANCELED'];
        const execVenues = ['NYSE', 'NASDAQ', 'BATS', 'IEX', 'ARCA'];
        const funds = ['Growth_Fund', 'Value_Fund', 'Index_Fund', 'Sector_Fund'];
        const benchmarks = ['S&P_500', 'NASDAQ_100', 'RUSSELL_2000', 'DOW_JONES'];
        const sectors = ['Technology', 'Healthcare', 'Financial', 'Consumer', 'Industrial'];
        const securityNames = {
            'AAPL.N': 'Apple Inc.',
            'MSFT.N': 'Microsoft Corporation',
            'GOOGL.N': 'Alphabet Inc.',
            'AMZN.N': 'Amazon.com Inc.',
            'TSLA.N': 'Tesla Inc.',
            'NVDA.N': 'NVIDIA Corporation',
            'META.N': 'Meta Platforms Inc.',
            'NFLX.N': 'Netflix Inc.'
        };

        for (let i = 0; i < numRecords; i++) {
            const symbol = symbols[Math.floor(Math.random() * symbols.length)];
            const basePrice = 100 + Math.random() * 400; // Price between 100-500
            const qty = Math.floor(Math.random() * 1000) + 100; // Quantity between 100-1100
            const price = +(basePrice + (Math.random() - 0.5) * 10).toFixed(2);
            const spreadPrice = +(price * 0.001).toFixed(4);
            const bidPrice = +(price - spreadPrice / 2).toFixed(2);
            const askPrice = +(price + spreadPrice / 2).toFixed(2);
            const midPrice = +((bidPrice + askPrice) / 2).toFixed(2);
            const tradeValue = +(qty * price).toFixed(2);

            // Generate realistic timestamp (within last 30 days, during trading hours)
            const daysAgo = Math.floor(Math.random() * 30);
            const tradingDate = new Date();
            tradingDate.setDate(tradingDate.getDate() - daysAgo);
            tradingDate.setHours(9 + Math.floor(Math.random() * 7), Math.floor(Math.random() * 60), Math.floor(Math.random() * 60), Math.floor(Math.random() * 1000));

            const record = {
                trade_id: 100000 + i,
                trader: traders[Math.floor(Math.random() * traders.length)],
                desk: desks[Math.floor(Math.random() * desks.length)],
                event_ts: tradingDate.toISOString(),
                symbol: symbol,
                security_name: securityNames[symbol] || `${symbol} Corp.`,
                sector_gics: sectors[Math.floor(Math.random() * sectors.length)],
                side: sides[Math.floor(Math.random() * sides.length)],
                order_type: orderTypes[Math.floor(Math.random() * orderTypes.length)],
                order_qty: qty,
                order_status: orderStatuses[Math.floor(Math.random() * orderStatuses.length)],
                limit_price: +(price * (1 + (Math.random() - 0.5) * 0.02)).toFixed(2),
                qty: qty,
                price: price,
                trade_value: tradeValue,
                commission: +(tradeValue * 0.001).toFixed(4),
                exec_venue: execVenues[Math.floor(Math.random() * execVenues.length)],
                venue_fee: +(tradeValue * 0.0005).toFixed(4),
                bid_price: bidPrice,
                ask_price: askPrice,
                mid_price: midPrice,
                spread_price: spreadPrice,
                high_day: +(price * (1 + Math.random() * 0.05)).toFixed(2),
                low_day: +(price * (1 - Math.random() * 0.05)).toFixed(2),
                fund: funds[Math.floor(Math.random() * funds.length)],
                benchmark_index: benchmarks[Math.floor(Math.random() * benchmarks.length)]
            };

            data.push(record);
        }

        return data;
    }

    /**
     * Get the next chunk of data
     */
    getNextChunk(chunkSize = DEFAULT_CHUNK_SIZE) {
        if (!this.isLoaded || this.data.length === 0) {
            return [];
        }

        const chunk = [];
        for (let i = 0; i < chunkSize; i++) {
            chunk.push({ ...this.data[this.currentIndex] });
            this.currentIndex = (this.currentIndex + 1) % this.data.length;
        }

        return chunk;
    }
}

/**
 * Convert data to JSON serializable format
 */
function convertToJsonSerializable(data) {
    return data.map(record => {
        const converted = {};
        for (const [key, value] of Object.entries(record)) {
            if (value instanceof Date) {
                converted[key] = value.toISOString();
            } else if (typeof value === 'number' && !isFinite(value)) {
                converted[key] = null;
            } else {
                converted[key] = value;
            }
        }
        return converted;
    });
}

/* -------------------------------------------------------------
 * Main function to connect to NATS and publish blotter data
 * onto the Jetstream.
 ----------------------------------------------------------------
 */
async function main() {
    // Parse command line arguments
    const args = process.argv.slice(2);
    let chunkSize = DEFAULT_CHUNK_SIZE;
    let interval = DEFAULT_INTERVAL;

    for (let i = 0; i < args.length; i++) {
        if (args[i] === '--chunk-size' && i + 1 < args.length) {
            chunkSize = parseInt(args[i + 1]);
            i++;
        } else if (args[i] === '--interval' && i + 1 < args.length) {
            interval = parseInt(args[i + 1]);
            i++;
        } else if (args[i] === '--help') {
            console.log(`
Usage: node jetstream_blotter_producer.js [options]

Options:
  --chunk-size <number>    Number of rows per chunk (default: ${DEFAULT_CHUNK_SIZE})
  --interval <number>      Interval in milliseconds (default: ${DEFAULT_INTERVAL})
  --help                   Show this help message
            `);
            return;
        }
    }

    console.log(`Configuration: chunk-size=${chunkSize}, interval=${interval}ms`);

    // Ensure parquet file is available
    await fetchBlotterDataFile();
    
    // Initialize data reader and load data
    const dataReader = new BlotterDataReader(LOCAL_BLOTTER_FILE);
    await dataReader.loadData();
    console.log("Blotter data reader initialized and data loaded");

    // Connect to NATS server
    const nc = await connect({ servers: NATS_SERVER_URL });
    const d = deferred();
    console.log(`Connected to NATS server: ${NATS_SERVER_URL}`);

    // Create a jetstream for blotter data
    const jsm = await nc.jetstreamManager();
    const jsName = "blotter";
    const jsSubName = `${jsName}_trades`;
    const jsc = {
        name: jsName,
        subjects: [`${jsSubName}.*`],
        storage: "memory",
        max_age: nanos(60 * 60 * 1_000_000), // 1 hour
    };

    // Clean up existing stream and create new one
    await jsm.streams.delete(jsName).catch(() => {});
    await jsm.streams.add(jsc);
    console.log(`Jetstream created: {name: ${jsName}, subjects: ${jsc.subjects.join(", ")}}`);
    
    // Create a Jetstream producer
    const js = nc.jetstream();
    let totalRowsPublished = 0;

    // Set up publishing interval
    const timer = setInterval(async () => {
        try {
            const dataChunk = dataReader.getNextChunk(chunkSize);
            if (dataChunk.length === 0) {
                console.log("No more data to publish");
                return;
            }

            // Convert to JSON serializable format
            const serializedData = convertToJsonSerializable(dataChunk);
            const payload = JSON.stringify(serializedData);
            
            const jsTopicName = `${jsSubName}.data_rows`;
            await js.publish(jsTopicName, payload);
            
            totalRowsPublished += dataChunk.length;
            console.log(`Published to subject.topic: "${jsTopicName}", chunk size: ${dataChunk.length}, total published: ${totalRowsPublished}`);
            
            // Log sample of first record for verification
            if (totalRowsPublished <= chunkSize) {
                console.log("Sample record:", JSON.stringify(serializedData[0], null, 2));
            }
        } catch (err) {
            console.error("Error publishing data:", err);
        }
    }, interval);

    // Stop the demo after RUN_DURATION
    setTimeout(() => {
        console.log("Demo timeout reached. Stopping the jetstream blotter publisher...");
        console.log(`Total rows published: ${totalRowsPublished}`);
        clearInterval(timer);
        d.resolve();
    }, RUN_DURATION);

    // Log stream info
    let info = await jsm.streams.info(jsc.name);
    console.log("Stream state:", info.state);
    
    // Clean up
    await d;
    await nc.drain();
    console.log("Connection closed");
}

// Error handling
process.on('uncaughtException', (error) => {
    console.error('Uncaught Exception:', error);
    process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
    process.exit(1);
});

// Run the main function
if (import.meta.url === `file://${process.argv[1]}`) {
    main().catch(console.error);
}
