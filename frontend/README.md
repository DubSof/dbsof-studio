# DBSOF web app

This package contains the browser shell for the DBSOF UI. It connects to a Flask
backend server running on `http://localhost:5757` for database operations.

## Development

> **Prerequisites**: You will need to have Yarn 2+ installed, and have run the
> `yarn install` command at the repo root to install the workspace dependencies.

To start the UI dev server:

```sh
yarn dev
```

The app is served at `http://localhost:3002/ui`.

**Note**: The UI requires the Flask backend server to be running. Start it from
the `server` directory:

```sh
cd ../server
python -m dbsof_server.app
# or
dbsof-server
```

The Flask backend runs on `http://localhost:5757` by default.

## UI Tests

> **Prerequisites**: 
> - [Playwright](https://playwright.dev) is used for browser automation
> - Python 3.10+ is required for the Flask backend
> - The Flask backend dependencies must be installed (run `uv sync` in the `server` directory)

The tests use [Playwright](https://playwright.dev) to run end-to-end tests across
multiple browsers (Chromium, Firefox, WebKit). The test setup automatically starts
the Flask backend server before running tests.

### Running tests

To run all tests:

```sh
yarn test
```

This will:
1. Start the Flask backend server automatically
2. Run tests in Chromium, Firefox, and WebKit browsers
3. Clean up and stop the backend server after tests complete

To run tests in headed mode (see the browser window):

```sh
yarn test --headed
```

To run tests for a specific browser:

```sh
yarn test --project=chromium
```

To run a specific test file:

```sh
yarn test flaskBackend
```

### Test Structure

The tests are located in the `tests/` directory:

- **`flaskBackend.spec.ts`** - API endpoint tests for the Flask backend
  - Tests instance management endpoints
  - Tests database schema endpoints
  - Tests SQL execution endpoints
  - Tests query history endpoints
  - Tests table data endpoints

- **`queryEditor.spec.ts`** - Query editor UI tests
- **`repl.spec.ts`** - REPL interface tests
- **`schema.spec.ts`** - Schema viewer tests
- **`clientSettings.spec.ts`** - Client settings UI tests

### Writing tests

Tests use Playwright's test framework with custom fixtures defined in `playwright/fixtures.ts`:

- **`flaskApi`** - HTTP client for making requests to the Flask backend
  ```typescript
  test("example", async ({flaskApi}) => {
    const response = await flaskApi.get("/instances");
    const data = await response.json();
    // ...
  });
  ```

- **`uiClass`** - Helper to locate elements by CSS module class name
  ```typescript
  test("example", async ({uiClass}) => {
    const button = uiClass("myComponent_button");
    await button.click();
  });
  ```

- **`mockClipboard`** - Mock clipboard for testing copy operations
  ```typescript
  test("example", async ({mockClipboard}) => {
    await button.click();
    expect(mockClipboard.getClipboardData()).toBe("expected");
  });
  ```

The Flask backend server is automatically started in `playwright/_globalSetup.ts` and
stopped in `playwright/_globalTeardown.ts`. Tests should not manually start/stop
the server.
