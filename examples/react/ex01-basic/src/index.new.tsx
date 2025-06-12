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
import * as psp from "@finos/perspective";
import * as pspViewer from "@finos/perspective-viewer";
import { PerspectiveViewer } from "@finos/perspective-react";
import "@finos/perspective-viewer-datagrid";
import "@finos/perspective-viewer-d3fc";
import "@finos/perspective-viewer/dist/css/themes.css";
import "./index.css";

import SERVER_WASM from "@finos/perspective/dist/wasm/perspective-server.wasm";
import CLIENT_WASM from "@finos/perspective-viewer/dist/wasm/perspective-viewer.wasm";
import SUPERSTORE_ARROW from "superstore-arrow/superstore.lz4.arrow";

/* ============================================================================
 * Perspective WASM & Client Initialization
 * 
 * See: https://perspective.finos.org/guide/how_to/javascript/importing.html
 * ============================================================================
 */

await Promise.all([
    psp.init_server(fetch(SERVER_WASM)),
    pspViewer.init_client(fetch(CLIENT_WASM)),
]);

const client: psp.Client = await psp.worker();

/* ============================================================================
 * Data Sources
 * 
 * - Superstore Arrow file
 * - Canned Viewer Configurations
 * ============================================================================
 */

const createNewSuperstorePerspectiveTable = async (): Promise<psp.Table> => {
    console.warn("Creating new superstore perscpective table!");
    const resp = await fetch(SUPERSTORE_ARROW);
    return client.table(await resp.arrayBuffer());
};


const CONFIG_DEFAULT: pspViewer.ViewerConfigUpdate = {
    theme: "Pro Dark",
};


const CONFIG_UPDATE: pspViewer.ViewerConfigUpdate = {
    plugin: "X Bar",
    settings: true,
    theme: "Pro Dark",
    title: null,
    group_by: ["Category", "Sub-Category"],
    split_by: ["Region"],
    columns: ["Sales"],
    aggregates: {
        Sales: "sum",
        Profit: "mean",
        Quantity: "sum",
    },
};

/* ============================================================================
 * React Application
 * 
 * This is the main React component that renders:
 * 1) Main Perspective Viewer React component
 * 2) Toolbar with buttons to demostate component's basic functionality
 * ============================================================================
 */

interface AppState {
    table: Promise<psp.Table> | undefined;
    config: pspViewer.ViewerConfigUpdate;
}

const App: React.FC = () => {
    const [state, setState] = React.useState<AppState>({
        table: createNewSuperstorePerspectiveTable(),
        config: CONFIG_DEFAULT,
    });

    React.useEffect(() => {
        return () => {
            state.table?.then((t) => t?.delete({ lazy: true }));
        };
    }, [state.table]);

    function applyConfig() {
        console.log("Applying config", CONFIG_UPDATE);
        setState((prev) => ({
            ...prev,
            config: { ...CONFIG_UPDATE },
        }));
    }

    function resetTable() {
        state.table?.then((t) => t?.delete({ lazy: true }));
        setState((prev) => ({
            ...prev,
            table: createNewSuperstorePerspectiveTable(),
        }));
    }

    function deleteTable() {
        state.table?.then((t) => t?.delete({ lazy: true }));
        setState((prev) => ({
            ...prev,
            table: undefined,
        }));
    }

    function handleConfigUpdate(newConfig: pspViewer.ViewerConfigUpdate) {
        console.log("Config Update Event", newConfig);
        // setState((prev) => ({
        //     ...prev,
        //     config: newConfig,
        // }));
    }

    function handleClick(detail: pspViewer.PerspectiveClickEventDetail) {
        console.log("Click Event,", detail);
    }

    function handleRowSelect(detail: pspViewer.PerspectiveSelectEventDetail) {
        console.log("Select Event", detail);
    }

    return (
        <div className="container">
            <div className="toolbar">
                <button onClick={applyConfig}>Set Config</button>
                {/* <button onClick={resetConfig}>Reset Config</button> */}
                <button onClick={resetTable}>Reset Table</button>
                <button onClick={deleteTable}>Delete Table</button>
            </div>
            <PerspectiveViewer
                table={state.table}
                config={state.config}
                onClick={handleClick}
                onSelect={handleRowSelect}
                onConfigUpdate={handleConfigUpdate}
            />
        </div>
    );
};

createRoot(document.getElementById("root")!).render(<App />);
