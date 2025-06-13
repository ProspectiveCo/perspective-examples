// ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
// ┃ ██████ ██████ ██████       █      █      █      █      █ █▄  ▀███ █       ┃
// ┃ ▄▄▄▄▄█ █▄▄▄▄▄ ▄▄▄▄▄█  ▀▀▀▀▀█▀▀▀▀▀ █ ▀▀▀▀▀█ ████████▌▐███ ███▄  ▀█ █ ▀▀▀▀▀ ┃
// ┃ █▀▀▀▀▀ █▀▀▀▀▀ █▀██▀▀ ▄▄▄▄▄ █ ▄▄▄▄▄█ ▄▄▄▄▄█ ████████▌▐███ █████▄   █ ▄▄▄▄▄ ┃
// ┃ █      ██████ █  ▀█▄       █ ██████      █      ███▌▐███ ███████▄ █       ┃
// ┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
// ┃ Copyright (c) 2017, the Perspective Authors.                              ┃
// ┃ ╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌  ┃
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

import SlButton from '@shoelace-style/shoelace/dist/react/button/index.js';
import SlButtonGroup from '@shoelace-style/shoelace/dist/react/button-group/index.js';
import "@shoelace-style/shoelace/dist/themes/light.css";

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

async function createSuperstoreTable(): Promise<psp.Table> {
    console.warn("Creating new superstore perspective table!");
    const resp = await fetch(SUPERSTORE_ARROW);
    return client.table(await resp.arrayBuffer());
};

const CONFIG: pspViewer.ViewerConfigUpdate = {
    group_by: ["State"],
    theme: "Pro Dark",
};

/* ============================================================================
 * React Application
 * 
 * This is the main React component that renders:
 * 1) Main Perspective Viewer React component
 * 2) Toolbar with a Shoelace ButtonGroup demonstrating basic functionality
 * ============================================================================
 */

interface ToolbarState {
    visible: boolean;
    table?: Promise<psp.Table>;
    config: pspViewer.ViewerConfigUpdate;
}

const App: React.FC = () => {
    const [state, setState] = React.useState<ToolbarState>(() => ({
        visible: true,
        table: createSuperstoreTable(),
        config: { ...CONFIG },
    }));

    React.useEffect(() => {
        return () => {
            state.table?.then((table) => table?.delete({ lazy: true }));
        };
    }, []);

    const onTableReset = () => {
        state.table?.then((table) => table?.delete({ lazy: true }));
        const table = createSuperstoreTable();
        setState({ ...state, table });
    };

    const onTableDelete = () => {
        state.table?.then((table) => table?.delete({ lazy: true }));
        setState({ ...state, table: undefined });
    };

    const onViewToggle = () =>
        setState((old) => ({ ...old, visible: !old.visible }));

    const onConfigUpdate = (config: pspViewer.ViewerConfigUpdate) => {
        console.log("Config Update Event", config);
        setState({ ...state, config });
    };

    const onViewClick = (detail: pspViewer.PerspectiveClickEventDetail) => {
        console.log("Click Event,", detail);
    };

    const onViewRowSelect = (detail: pspViewer.PerspectiveSelectEventDetail) => {
        console.log("Select Event", detail);
    };

    return (
        <div className="container">
            <header className="toolbar">
                <h1>Perspective React Example</h1>
                <p>Demonstrates basic functionality of perspective-react</p>
                <SlButtonGroup>
                    <SlButton variant="primary" onClick={onViewToggle}>Toggle Mount</SlButton>
                    <SlButton variant="success" onClick={onTableReset}>Reset Table</SlButton>
                    <SlButton variant="danger" onClick={onTableDelete}>Delete Table</SlButton>
                </SlButtonGroup>
            </header>
            <div className="perspective-container">
                {state.visible && (
                    <PerspectiveViewer
                        table={state.table}
                        config={state.config}
                        onClick={onViewClick}
                        onSelect={onViewRowSelect}
                        onConfigUpdate={onConfigUpdate}
                    />
                )}
            </div>
        </div>
    );
};

createRoot(document.getElementById("root")!).render(<App />);
