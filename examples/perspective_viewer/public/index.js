import "https://cdn.jsdelivr.net/npm/@finos/perspective-viewer@3.4.3/dist/cdn/perspective-viewer.js";
import "https://cdn.jsdelivr.net/npm/@finos/perspective-viewer-datagrid@3.4.3/dist/cdn/perspective-viewer-datagrid.js";
import "https://cdn.jsdelivr.net/npm/@finos/perspective-viewer-d3fc@3.4.3/dist/cdn/perspective-viewer-d3fc.js";

import perspective from "https://cdn.jsdelivr.net/npm/@finos/perspective@3.4.3/dist/cdn/perspective.js";


// get perspective-viewer element
const viewer = document.getElementById("viewer");

// fetch sample data: superstore contains example online retail data -- arrow format
const resp = await fetch("https://cdn.jsdelivr.net/npm/superstore-arrow/superstore.lz4.arrow");
const arrow = await resp.arrayBuffer();

// create a perspective worker and load the arrow data into it
const client = await perspective.worker();
const table = client.table(arrow);
viewer.load(table);

// pre-configure the viewer with a default view configuration
const viewer_config = {
    version: "3.4.3",
    plugin: "Datagrid",
    plugin_config: {
        columns: {},
        edit_mode: "READ_ONLY",
        scroll_lock: false
    },
    columns_config: {},
    settings: true,
    theme: "Pro Dark",
    title: null,
    group_by: ["Category", "Sub-Category"],
    split_by: [],
    columns: ["Customer ID", "Ship Date", "Quantity", "Sales", "Profit", "City"],
    filter: [],
    sort: [],
    expressions: {},
    aggregates: {
        City: "dominant",
        "Ship Date": "last"
    }
};
viewer.restore(viewer_config);
