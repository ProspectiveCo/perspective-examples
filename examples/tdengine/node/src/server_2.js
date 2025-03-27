import { WebSocketServer, table } from "@finos/perspective";

// Perspective configuration
const PERSPECTIVE_TABLE_NAME = 'people';

/**
 * Create a Perspective table and host it via WebSocket.
 */
async function createPerspectiveServer() {
    const schema = {
        name: "string",
        age: "integer",
    };
    const data = [{name: "John", age: 25}, {name: "Jane", age: 24}];
    let perspectiveTable = table(schema, { limit: 1000, format: "json" });
    await perspectiveTable.update(data);

    // Start a WebSocket server on port 8080
    const host = new WebSocketServer({ port: 8080 });
    host.host_table(PERSPECTIVE_TABLE_NAME, perspectiveTable);
    console.log(`Perspective WebSocket server is running on ws://localhost:8080`);
}

/**
 * Main function to orchestrate the workflow.
 */
async function main() {
    await createPerspectiveServer();
}

main();
