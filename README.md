# IRON VEIL V2.0

This package contains the single-file game client, the Node.js WebSocket server, and a server integration test. The client adds low-profile themed route lights to each map, extra handguard and optic details to the first-person rifle, and a more detailed armored online player model. Repeated player geometry and materials are shared to limit memory use. Existing training dummies and map layouts are preserved.

## Server address and deployment

The client defaults to `wss://iron-veil-v20-server.onrender.com`. The address is also editable in the multiplayer lobby and can be overridden with the `server` query parameter.

`render.yaml` configures the Render service name, Node runtime, install and start commands, health check, and match settings. The Render service itself must be connected to a GitHub repository containing these files. Updating this ZIP does not change an already deployed Render service; publish the updated `server.js`, `package.json`, lockfile, and `render.yaml` to the repository Render deploys from, or upload them through the deployment flow you use.

The HTML client is a separate static page. Publish `게임클라이언트_V2.0.html` to the static hosting location used by players. No GitHub Pages repository or custom domain is configured by this package.

## Run the server locally

Requires Node.js 20 or later.

```sh
npm install
npm start
```

The health endpoint is available at `http://localhost:3000/health`.

## Verify the server

```sh
npm test
```

The integration test starts a local server and checks channel listing, nickname validation, room creation and joining, quick matching, one-vote-per-player tally, host transfer, manual match start, and host reassignment after leaving. Browser rendering and live Render/GitHub deployment are not exercised by this test.
