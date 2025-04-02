import { connect, deferred } from "@nats-io/transport-node";

// NATS configuration
const NATS_SERVER_URL = "nats://localhost:4222";
const NATS_SUBJECT = "meters";

// Demo parameters
const LOCATIONS = ["San Francisco", "Los Angeles", "San Diego", "San Jose", "Palo Alto", "Campbell", "Mountain View", "Sunnyvale", "Santa Clara", "Cupertino"];
const PHASE = ["A", "B", "C"];
const NUM_ROWS_PER_INTERVAL = 10;           // max number of rows to generate per interval 
const INTERVAL = 250;                       // milliseconds
const DEMO_RUN_TIME = 30 * 60 * 1000;       // minutes


/**
 * Generates random power-line data
 */
function generateData(num_rows = NUM_ROWS_PER_INTERVAL) {
    const modifier = Math.random() * (Math.random() * 50 + 1);
    return Array.from({ length: num_rows }, (_, i) => ({
        ts: new Date().toISOString(),
        current: Math.random() * 75 + Math.random() * 10 * modifier,
        voltage: Math.floor(Math.random() * 26) + 200,
        phase: PHASE[Math.floor(Math.random() * PHASE.length)],
        location: LOCATIONS[Math.floor(Math.random() * LOCATIONS.length)],
    }));
}


async function main() {
    // connect to NATS server
    const nc = await connect({ servers: NATS_SERVER_URL });
    const d = deferred();
    console.log(`connected to nats server: ${NATS_SERVER_URL}`);

    const timer = setInterval(async () => {
        try {
            // generate random data
            const num_rows = Math.floor(Math.random() * NUM_ROWS_PER_INTERVAL) + 1;
            const data = generateData(num_rows);
            // publish the data to NATS
            const payload = JSON.stringify(data);
            await nc.publish(NATS_SUBJECT, payload);
            console.log(`Published to subject: "${NATS_SUBJECT}", num rows: ${num_rows}`);
        } catch (err) {}
    }, INTERVAL);

    setTimeout(() => {
        console.log("Demo timeout reached. Stopping the publisher...");
        clearInterval(timer);
        d.resolve();
    }, DEMO_RUN_TIME); // Stop after X minutes

    await d;
    await nc.drain();
}


main();
