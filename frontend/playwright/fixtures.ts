import {test as base, type Locator} from "@playwright/test";

const FLASK_SERVER_URL = "http://localhost:5757";

type FlaskApiClient = {
  get: (path: string) => Promise<Response>;
  post: (path: string, body?: any) => Promise<Response>;
  instanceId: string;
  db: string;
};

type Fixtures = {
  flaskApi: FlaskApiClient;
  uiClass: (className: string) => Locator;
  mockClipboard: {
    getClipboardData: () => string;
  };
};

export const test = base.extend<Fixtures>({
  flaskApi: ({}, use) => {
    const client: FlaskApiClient = {
      instanceId: "demo",
      db: "main",
      async get(path: string) {
        const url = `${FLASK_SERVER_URL}${path}`;
        return fetch(url);
      },
      async post(path: string, body?: any) {
        const url = `${FLASK_SERVER_URL}${path}`;
        return fetch(url, {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: body ? JSON.stringify(body) : undefined,
        });
      },
    };
    use(client);
  },
  uiClass: ({page}, use) => {
    use((className: string) => page.locator(`[class*=${className}__]`));
  },
  mockClipboard: async ({page}, use) => {
    let clipboardData = "";
    await page.exposeFunction(
      "_mockClipboardWrite",
      (data: string) => (clipboardData = data)
    );
    await page.addInitScript(() => {
      // create a mock of the clipboard API
      const mockClipboard = {
        clipboardData: "",
        writeText: async (text: string) => {
          mockClipboard.clipboardData = text;
          // @ts-expect-error
          _mockClipboardWrite(text);
        },
        readText: async () => mockClipboard.clipboardData,
      };

      // override the native clipboard API
      Object.defineProperty(navigator, "clipboard", {
        value: mockClipboard,
        writable: false,
        enumerable: true,
        configurable: true,
      });
    });

    await use({
      getClipboardData() {
        return clipboardData;
      },
    });
  },
});
