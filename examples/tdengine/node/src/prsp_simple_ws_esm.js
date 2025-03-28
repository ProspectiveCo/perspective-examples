import { fileURLToPath } from 'url';
import { dirname } from 'path';
import perspective from "@finos/perspective";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

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

    // Start a WebSocket server on port 8080
    const host = new perspective.WebSocketServer({ assets: [__dirname], port: 8080 });
    
    let prspTable = await perspective.table(schema, { name: PERSPECTIVE_TABLE_NAME, limit: 1000, format: "json" });
    await prspTable.update(data);

    console.log(`Perspective WebSocket server is running on ws://localhost:8080`);
}

/**
 * Main function to orchestrate the workflow.
 */
async function main() {
    await createPerspectiveServer();
}

main();
