import { connect, deferred, nanos } from "nats.ws";

// NATS configuration
const NATS_SERVER_URL = "ws://localhost:8080";


async function main() {
    // connect to NATS server
    const nc = await connect({ servers: NATS_SERVER_URL });
    const d = deferred();
    console.log(`connected to nats server: ${NATS_SERVER_URL}`);

    const jsm = await nc.jetstreamManager();
    const jsName = "meters";
    const subName = `${jsName}_sub`;
    const jsc = {
        name: jsName,
        subjects: [`${subName}.*`],
        storage: "memory",
        max_age: nanos(60 * 60 * 1000000), // 1 hour
    };
    await jsm.streams.delete(jsName).catch(() => {});
    await jsm.streams.add(jsc);
    console.log(`Jetstream created: {name: ${jsName}, subjects: ${jsc.subjects.join(", ")}}`);
    const js = nc.jetstream();

    const events = [
        `${subName}.input_changed`,
        `${subName}.input_blurred`,
        `${subName}.key_pressed`,
        `${subName}.input_focused`,
        `${subName}.input_changed`,
        `${subName}.input_blurred`,
    ];
    const batch = events.map((e) => js.publish(e, JSON.stringify({ ts: new Date().toISOString(), event: e })));
    await Promise.all(batch);
    console.log(`Published ${events.length} events to Jetstream`);

    let info = await jsm.streams.info(jsc.name);
    console.log(info.state);
    
    d.resolve();
    await d;
    await nc.drain();
}


main();
