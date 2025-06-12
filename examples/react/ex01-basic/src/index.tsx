// ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
// ┃ ██████ ██████ ██████       █      █      █      █      █ █▄  ▀███ █       ┃
// ┃ ▄▄▄▄▄█ █▄▄▄▄▄ ▄▄▄▄▄█  ▀▀▀▀▀█▀▀▀▀▀ █ ▀▀▀▀▀█ ████████▌▐███ ███▄  ▀█ █ ▀▀▀▀▀ ┃
// ┃ █▀▀▀▀▀ █▀▀▀▀▀ █▀██▀▀ ▄▄▄▄▄ █ ▄▄▄▄▄█ ▄▄▄▄▄█ ████████▌▐███ █████▄   █ ▄▄▄▄▄ ┃
// ┃ █      ██████ █  ▀█▄       █ ██████      █      ███▌▐███ ███████▄ █       ┃
// ┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
// ┃ Copyright (c) 2017, the Perspective Authors.                              ┃
// ┃ ╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌ ┃
// ┃ This file is part of the Perspective library, distributed under the terms ┃
// ┃ of the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). ┃
// ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛


// Imports
import * as React from "react";
import { createRoot } from "react-dom/client";
import psp from "@finos/perspective";
import pspViewer from "@finos/perspective-viewer";
import { PerspectiveViewer } from "@finos/perspective-react";
import "@finos/perspective-viewer-datagrid";
import "@finos/perspective-viewer-d3fc";
import "@finos/perspective-viewer/dist/css/themes.css";
import "./index.css";

import SERVER_WASM from "@finos/perspective/dist/wasm/perspective-server.wasm";
import CLIENT_WASM from "@finos/perspective-viewer/dist/wasm/perspective-viewer.wasm";
import SUPERSTORE_ARROW from "superstore-arrow/superstore.lz4.arrow";

// # [Perspective bootstrapping](https://perspective.finos.org/guide/how_to/javascript/importing.html)
// Here we're initializing the WASM interpreter that powers the perspective API
// This example is written assuming that the bundler is configured

await Promise.all([
    psp.init_server(fetch(SERVER_WASM)),
    pspViewer.init_client(fetch(CLIENT_WASM)),
]);


// # Data Source

// Data source creates a static Web Worker instance of Perspective engine, and a
// table creation function which both downloads data and loads it into the
// engine.

const WORKER = await psp.worker();

async function createNewSuperstoreTable(): Promise<psp.Table> {
    console.warn("Creating new table!");
    const req = fetch(SUPERSTORE_ARROW);
    const resp = await req;
    const buffer = await resp.arrayBuffer();
    return await WORKER.table(buffer);
}

const CONFIG: pspViewer.ViewerConfigUpdate = {
    group_by: ["State"],
};

// # React application

// The React application itself

interface ToolbarState {
    mounted: boolean;
    table?: Promise<psp.Table>;
    config: pspViewer.ViewerConfigUpdate;
}

const App: React.FC = () => {
    const [state, setState] = React.useState<ToolbarState>(() => ({
        mounted: true,
        table: createNewSuperstoreTable(),
        config: { ...CONFIG },
    }));

    React.useEffect(() => {
        return () => {
            state.table?.then((table) => table?.delete({ lazy: true }));
        };
    }, []);

    const onClickOverwrite = () => {
        state.table?.then((table) => table?.delete({ lazy: true }));
        const table = createNewSuperstoreTable();
        setState({ ...state, table });
    };

    const onClickDelete = () => {
        state.table?.then((table) => table?.delete({ lazy: true }));
        setState({ ...state, table: undefined });
    };

    const onClickToggleMount = () =>
        setState((old) => ({ ...old, mounted: !state.mounted }));

    const onConfigUpdate = (config: pspViewer.ViewerConfigUpdate) => {
        console.log("Config Update Event", config);
        setState({ ...state, config });
    };

    const onClick = (detail: pspViewer.PerspectiveClickEventDetail) => {
        console.log("Click Event,", detail);
    };

    const onSelect = (detail: pspViewer.PerspectiveSelectEventDetail) => {
        console.log("Select Event", detail);
    };

    return (
        <div className="container">
            <div className="toolbar">
                <button onClick={onClickToggleMount}>Toggle Mount</button>
                <button onClick={onClickOverwrite}>Overwrite Superstore</button>
                <button onClick={onClickDelete}>Delete Table</button>
            </div>
            {state.mounted && (
                <>
                    <PerspectiveViewer table={state.table} />
                    <PerspectiveViewer
                        table={state.table}
                        config={state.config}
                        onClick={onClick}
                        onSelect={onSelect}
                        onConfigUpdate={onConfigUpdate}
                    />
                </>
            )}
        </div>
    );
};

createRoot(document.getElementById("root")!).render(<App />);
