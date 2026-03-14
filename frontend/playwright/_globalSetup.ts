import {spawn, type ChildProcess} from "node:child_process";
import {join} from "node:path";

const FLASK_SERVER_URL = "http://localhost:5757";

async function waitForServer(url: string, timeout = 30000): Promise<void> {
  const startTime = Date.now();
  while (Date.now() - startTime < timeout) {
    try {
      const response = await fetch(url);
      if (response.ok || response.status === 404) {
        return;
      }
    } catch {
      // Server not ready yet
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(`Server at ${url} did not become available within ${timeout}ms`);
}

export default async function globalSetup() {
  console.log("Starting Flask backend server...");

  // Determine server directory path
  const webDir = process.cwd();
  const serverDir = join(webDir, "..", "server", "src");

  // Start Flask server
  // Use dbsof-server command if available, otherwise fall back to python module
  const flaskServer = spawn("python", ["-m", "dbsof_server.app"], {
    cwd: serverDir,
    stdio: "pipe",
    shell: true,
    env: {...process.env, PYTHONPATH: serverDir},
  });

  // Store process globally for teardown
  (globalThis as any).backendServerProc = flaskServer;

  let serverOutput = "";
  flaskServer.stdout?.on("data", (data) => {
    const output = data.toString();
    serverOutput += output;
    if (output.includes("Running on") || output.includes(" * Running on")) {
      console.log("Flask server:", output.trim());
    }
  });

  flaskServer.stderr?.on("data", (data) => {
    const output = data.toString();
    // Filter out common Flask debug messages
    if (
      !output.includes("WARNING") &&
      !output.includes("Debug mode") &&
      !output.includes("This is a development server")
    ) {
      console.error("Flask server error:", output.trim());
    }
  });

  flaskServer.on("error", (error) => {
    console.error("Failed to start Flask server:", error);
    console.error("Server output so far:", serverOutput);
  });

  // Wait for server to be ready
  try {
    await waitForServer(`${FLASK_SERVER_URL}/instances`);
    console.log("Flask backend server is ready at", FLASK_SERVER_URL);
  } catch (error) {
    console.error("Failed to start Flask server:", error);
    console.error("Server output:", serverOutput);
    flaskServer.kill();
    throw error;
  }
}
