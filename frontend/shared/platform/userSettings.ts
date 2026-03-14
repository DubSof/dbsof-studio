/**
 * User Settings API
 * Functions to fetch and update user settings from the server
 */

export interface UserSettings {
  [key: string]: any;
}

/**
 * Fetch user settings from the server
 * @param serverUrl Base URL of the server
 * @param userId User identifier (username)
 * @returns Promise resolving to user settings object
 */
export async function getUserSettings(
  serverUrl: string,
  userId: string
): Promise<UserSettings> {
  const response = await fetch(
    `${serverUrl}/users/${encodeURIComponent(userId)}/settings`
  );

  if (!response.ok) {
    if (response.status === 404) {
      return {};
    }
    const errorData = await response.json().catch(() => ({
      error: `HTTP ${response.status}: Failed to fetch user settings`,
    }));
    throw new Error(errorData.error || "Failed to fetch user settings");
  }

  return (await response.json()) || {};
}

/**
 * Update user settings on the server
 * Merges the provided settings with existing settings
 * @param serverUrl Base URL of the server
 * @param userId User identifier (username)
 * @param settings Settings object to update (will be merged with existing)
 * @returns Promise resolving to the updated settings object
 */
export async function updateUserSettings(
  serverUrl: string,
  userId: string,
  settings: UserSettings
): Promise<UserSettings> {
  const response = await fetch(
    `${serverUrl}/users/${encodeURIComponent(userId)}/settings`,
    {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(settings),
    }
  );

  if (!response.ok) {
    const errorData = await response.json().catch(() => ({
      error: `HTTP ${response.status}: Failed to update user settings`,
    }));
    throw new Error(errorData.error || "Failed to update user settings");
  }

  return await response.json();
}
