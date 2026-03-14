import {test, expect} from "../playwright";

test.describe("Flask Backend API", () => {
  test("GET /instances - should return demo instance", async ({flaskApi}) => {
    const response = await flaskApi.get("/instances");
    expect(response.ok).toBe(true);
    const data = await response.json();
    expect(Array.isArray(data)).toBe(true);
    expect(data.length).toBeGreaterThan(0);
    expect(data[0]).toHaveProperty("id");
    expect(data[0]).toHaveProperty("name");
  });

  test("GET /instances/{id}/databases - should return databases list", async ({
    flaskApi,
  }) => {
    const response = await flaskApi.get(
      `/instances/${flaskApi.instanceId}/databases`
    );
    expect(response.ok).toBe(true);
    const data = await response.json();
    expect(Array.isArray(data)).toBe(true);
    // Should have at least the main database
    expect(data.length).toBeGreaterThan(0);
    expect(data[0]).toHaveProperty("name");
  });

  test("GET /instances/{id}/databases/{db}/schema - should return schema", async ({
    flaskApi,
  }) => {
    const response = await flaskApi.get(
      `/instances/${flaskApi.instanceId}/databases/${flaskApi.db}/schema`
    );
    expect(response.ok).toBe(true);
    const data = await response.json();
    expect(data).toHaveProperty("types");
    expect(data).toHaveProperty("version");
    expect(Array.isArray(data.types)).toBe(true);
  });

  test("GET /instances/{id}/databases/{db}/tables - should return tables list", async ({
    flaskApi,
  }) => {
    const response = await flaskApi.get(
      `/instances/${flaskApi.instanceId}/databases/${flaskApi.db}/tables`
    );
    expect(response.ok).toBe(true);
    const data = await response.json();
    expect(Array.isArray(data)).toBe(true);
    // Should have seeded tables
    if (data.length > 0) {
      expect(data[0]).toHaveProperty("name");
      expect(data[0]).toHaveProperty("rowCount");
      expect(data[0]).toHaveProperty("columns");
    }
  });

  test("POST /instances/{id}/databases/{db}/sql/commands - should execute SQL query", async ({
    flaskApi,
  }) => {
    const response = await flaskApi.post(
      `/instances/${flaskApi.instanceId}/databases/${flaskApi.db}/sql/commands`,
      {
        query: "SELECT 1 + 1 AS result",
        mode: "tabular",
      }
    );
    expect(response.ok).toBe(true);
    const data = await response.json();
    expect(data).toHaveProperty("status");
    expect(data.status).toBe("OK");
    expect(data).toHaveProperty("columns");
    expect(data).toHaveProperty("rows");
    expect(data.columns).toContain("result");
    expect(data.rows.length).toBeGreaterThan(0);
    expect(data.rows[0]).toContain(2);
  });

  test("POST /instances/{id}/databases/{db}/sql/commands - should handle invalid SQL", async ({
    flaskApi,
  }) => {
    const response = await flaskApi.post(
      `/instances/${flaskApi.instanceId}/databases/${flaskApi.db}/sql/commands`,
      {
        query: "SELECT * FROM nonexistent_table",
        mode: "tabular",
      }
    );
    // Should return error status
    expect(response.status).toBe(400);
    const data = await response.json();
    expect(data).toHaveProperty("error");
  });

  test("GET /instances/{id}/databases/{db}/sql/history - should return query history", async ({
    flaskApi,
  }) => {
    // First execute a query to create history
    await flaskApi.post(
      `/instances/${flaskApi.instanceId}/databases/${flaskApi.db}/sql/commands`,
      {
        query: "SELECT 1",
        mode: "tabular",
      }
    );

    const response = await flaskApi.get(
      `/instances/${flaskApi.instanceId}/databases/${flaskApi.db}/sql/history`
    );
    expect(response.ok).toBe(true);
    const data = await response.json();
    expect(data).toHaveProperty("items");
    expect(data).toHaveProperty("nextCursor");
    expect(Array.isArray(data.items)).toBe(true);
    if (data.items.length > 0) {
      expect(data.items[0]).toHaveProperty("id");
      expect(data.items[0]).toHaveProperty("query");
      expect(data.items[0]).toHaveProperty("status");
      expect(data.items[0]).toHaveProperty("durationMs");
    }
  });

  test("GET /instances/{id}/databases/{db}/tables/{table}/schema - should return table schema", async ({
    flaskApi,
  }) => {
    // First check if Customer table exists
    const tablesResponse = await flaskApi.get(
      `/instances/${flaskApi.instanceId}/databases/${flaskApi.db}/tables`
    );
    const tables = await tablesResponse.json();
    const customerTable = tables.find((t: any) => t.name === "Customer");

    if (customerTable) {
      const response = await flaskApi.get(
        `/instances/${flaskApi.instanceId}/databases/${flaskApi.db}/tables/Customer/schema`
      );
      expect(response.ok).toBe(true);
      const data = await response.json();
      expect(data).toHaveProperty("name");
      expect(data).toHaveProperty("columns");
      expect(data).toHaveProperty("primaryKey");
      expect(data.name).toBe("Customer");
      expect(Array.isArray(data.columns)).toBe(true);
    }
  });

  test("GET /instances/{id}/databases/{db}/tables/{table}/rows - should return table rows", async ({
    flaskApi,
  }) => {
    // First check if Customer table exists
    const tablesResponse = await flaskApi.get(
      `/instances/${flaskApi.instanceId}/databases/${flaskApi.db}/tables`
    );
    const tables = await tablesResponse.json();
    const customerTable = tables.find((t: any) => t.name === "Customer");

    if (customerTable) {
      const response = await flaskApi.get(
        `/instances/${flaskApi.instanceId}/databases/${flaskApi.db}/tables/Customer/rows?limit=10`
      );
      expect(response.ok).toBe(true);
      const data = await response.json();
      expect(data).toHaveProperty("columns");
      expect(data).toHaveProperty("rows");
      expect(data).toHaveProperty("total");
      expect(Array.isArray(data.columns)).toBe(true);
      expect(Array.isArray(data.rows)).toBe(true);
      expect(typeof data.total).toBe("number");
    }
  });
});
