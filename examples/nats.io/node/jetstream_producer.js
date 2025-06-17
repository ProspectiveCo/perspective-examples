import { connect, deferred, nanos } from "nats.ws";

// NATS configuration
const NATS_SERVER_URL = "ws://localhost:8080";
// Data configuration
const RUN_DURATION = 3 * 60 * 1000;    // 1 hour in milliseconds
const INTERVAL = 250;                       // interval in milliseconds
const ROWS_PER_INTERVAL = 100;          // rows generated per interval


/* -------------------------------------------------------------
    * Data generation functions
 ----------------------------------------------------------------
 */

const COMPANY_METADATA = [
    { ticker: "AAPL.N", sector: "Information Technology", state: "CA", index_fund: ["S&P 500","NASDAQ 100","DJIA","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [169.21, 260.10], avg_volume: 594500 },
    { ticker: "AMZN.N", sector: "Consumer Discretionary", state: "WA", index_fund: ["S&P 500","NASDAQ 100","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [151.61, 242.52], avg_volume: 505200 },
    { ticker: "NVDA.N", sector: "Information Technology", state: "CA", index_fund: ["S&P 500","NASDAQ 100","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [86.62, 153.13], avg_volume: 2960000 },
    { ticker: "TSLA.N", sector: "Consumer Discretionary", state: "TX", index_fund: ["S&P 500","NASDAQ 100","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [167.41, 488.54], avg_volume: 1230000 },
    { ticker: "MSFT.N", sector: "Information Technology", state: "WA", index_fund: ["S&P 500","NASDAQ 100","DJIA","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [344.79, 468.35], avg_volume: 230000 },
    { ticker: "GOOGL.N", sector: "Communication Services", state: "CA", index_fund: ["S&P 500","NASDAQ 100","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [142.66, 208.70], avg_volume: 383200 },
    { ticker: "JPM.N", sector: "Financials", state: "NY", index_fund: ["S&P 500","DJIA","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [190.88, 280.25], avg_volume: 113800 },
    { ticker: "V.N", sector: "Financials", state: "CA", index_fund: ["S&P 500","DJIA","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [252.70, 366.54], avg_volume: 76000 },
    { ticker: "DIS.N", sector: "Communication Services", state: "CA", index_fund: ["S&P 500","DJIA","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [80.10, 118.63], avg_volume: 110000 },
    { ticker: "WMT.N", sector: "Consumer Staples", state: "AR", index_fund: ["S&P 500","DJIA","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [59.44, 105.30], avg_volume: 253000 },
    { ticker: "PFE.N", sector: "Health Care", state: "NY", index_fund: ["S&P 500","DJIA","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [20.92, 31.54], avg_volume: 551000 },
    { ticker: "ORCL.N", sector: "Information Technology", state: "TX", index_fund: ["S&P 500","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [114.55, 198.31], avg_volume: 112000 },
    { ticker: "NFLX.N", sector: "Communication Services", state: "CA", index_fund: ["S&P 500","NASDAQ 100","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [587.04, 1164.00], avg_volume: 41000 },
    { ticker: "INTC.N", sector: "Information Technology", state: "CA", index_fund: ["S&P 500","NASDAQ 100","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [17.67, 37.16], avg_volume: 1044000 },
    { ticker: "ADBE.N", sector: "Information Technology", state: "CA", index_fund: ["S&P 500","NASDAQ 100","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [332.01, 587.75], avg_volume: 41000 },
];

const CLIENTS = [
    "BlackRock", "Vanguard", "State Street",
    "Fidelity", "Goldman Sachs", "Morgan Stanley",
    "Citadel Securities", "Bridgewater", "Berkshire Hathaway",
];


/**
 * Returns the current date and time in ISO 8601 format with a random jitter of ±5 seconds.
 */
function _nowISO() {
    const offset = (Math.random() * 10) - 5; // jitter +/-5 sec
    return new Date(Date.now() + offset * 1000).toISOString();
}


/**
 * Generates a random number following a normal (Gaussian) distribution using the Box-Muller transform.
 */
function _randomDistribution(mean, stddev) {
    let u = 0, v = 0;
    while(u === 0) u = Math.random();
    while(v === 0) v = Math.random();
    let num = Math.sqrt(-2.0 * Math.log(u)) * Math.cos(2.0 * Math.PI * v);
    return num * stddev + mean;
}


/**
 * Generates an array of random market data objects for a given number of rows.
 */
function generateData(num_rows = ROWS_PER_INTERVAL) {
    const results = [];
    for (let i = 0; i < num_rows; i++) {
        // --- random company
        const metaIdx = Math.floor(Math.random() * COMPANY_METADATA.length);
        const meta = COMPANY_METADATA[metaIdx];
        // --- price bars
        const [lowRange, highRange] = meta.price_range;
        const open = +(Math.random() * (highRange - lowRange) + lowRange).toFixed(2);
        const high = +(open * (1 + Math.random() * 0.03)).toFixed(2);
        const low = +(open * (0.97 + Math.random() * 0.03)).toFixed(2);
        const close = +(Math.random() * (high - low) + low).toFixed(2);
        // --- market metrics
        const avgVol = meta.avg_volume;
        const volume = Math.max(1, Math.round(_randomDistribution(avgVol, avgVol * 0.15)));
        const lotSize = Math.floor(Math.random() * (500 - 50)) + 50;
        const trade_count = Math.floor(volume / lotSize);
        const notional = +(close * volume).toFixed(2);
        // --- dimensions & timestamps
        const index_fund = meta.index_fund[Math.floor(Math.random() * meta.index_fund.length)];
        const client = CLIENTS[Math.floor(Math.random() * CLIENTS.length)];
        const country = "United States";
        const trade_date = (new Date()).toISOString().slice(0, 10);
        const last_update = _nowISO();
        // append the row
        results.push({
            ticker: meta.ticker, sector: meta.sector, state: meta.state, 
            index_fund, open, high, low, close, volume, trade_count, 
            notional, client, country, trade_date, last_update
        });
    }
    return results;
}


/* -------------------------------------------------------------
 * Main function to connect to NATS and publish data
 * onto the Jetstream.
 ----------------------------------------------------------------
 */
async function main() {
    // connect to NATS server
    const nc = await connect({ servers: NATS_SERVER_URL });
    const d = deferred();
    console.log(`connected to nats server: ${NATS_SERVER_URL}`);

    // create a jetstream
    const jsm = await nc.jetstreamManager();
    const jsName = "markets";
    const jsSubName = `${jsName}_sub`;
    const jsc = {
        name: jsName,
        subjects: [`${jsSubName}.*`],
        storage: "memory",
        max_age: nanos(60 * 60 * 1_000_000), // 1 hour
    };
    await jsm.streams.delete(jsName).catch(() => {});
    await jsm.streams.add(jsc);
    console.log(`Jetstream created: {name: ${jsName}, subjects: ${jsc.subjects.join(", ")}}`);
    
    // create a Jetstream producer
    const js = nc.jetstream();
    const timer = setInterval(async () => {
        try {
            const numRows = Math.floor(Math.random() * ROWS_PER_INTERVAL) + 1;
            const dataRows = generateData(numRows);
            const payload = JSON.stringify(dataRows);
            const jsTopicName = `${jsSubName}.data_rows`;
            await js.publish(jsTopicName, payload);
            console.log(`Published to subject.topic: "${jsTopicName}", num rows: ${numRows}`);
        } catch (err) {}
    }, INTERVAL);

    // stop the demo after MAX_RUN_DURATION
    setTimeout(() => {
        console.log("Demo timeout reached. Stopping the jetstream publisher loop...");
        clearInterval(timer);
        d.resolve();
    }, RUN_DURATION);

    // clean up
    let info = await jsm.streams.info(jsc.name);
    console.log(info.state);
    await d;
    await nc.drain();
}


main();
