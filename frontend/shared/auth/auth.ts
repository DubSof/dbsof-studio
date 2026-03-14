import {AuthProvider, Role} from "@dbsof/studio/state/connection";
import {mockMode} from "@dbsof/platform/client";

export interface AuthUser {
  username: string;
  token: string;
  role?: Role;
}

export interface AuthConfig {
  defaultAccount?: {
    username: string;
    token: string;
  };
  storageKeys?: {
    token: string;
    username: string;
  };
  loginPath?: string;
  redirectAfterLogin?: string;
  serverUrl?: string;
}

export interface AuthLoginFunction {
  (serverUrl: string, username: string, password: string): Promise<string>;
}

/**
 * Centralized authentication manager
 * Handles all authentication logic including token storage, user management,
 * and default account fallback.
 */
export class AuthManager {
  private config: Required<AuthConfig>;
  private currentUser: AuthUser | null = null;
  private loginFunction: AuthLoginFunction | null = null;

  constructor(config: AuthConfig = {}) {
    this.config = {
      storageKeys: {
        token: "dbsofAuthToken",
        username: "dbsofAuthUsername",
      },
      loginPath: "/ui/_login",
      redirectAfterLogin: "/ui",
      serverUrl: "http://localhost:5757",
      ...config,
    };
    this.initialize();
  }

  /**
   * Set the login function to use for authentication
   */
  setLoginFunction(loginFn: AuthLoginFunction) {
    this.loginFunction = loginFn;
  }

  /**
   * Initialize authentication state from storage or URL params
   */
  private initialize() {
    const url = new URL(window.location.toString());
    const {token: tokenKey, username: usernameKey} = this.config.storageKeys;

    // Check for auth token in URL (for OAuth callbacks, etc.)
    if (url.searchParams.has("authToken")) {
      const token = url.searchParams.get("authToken")!;
      localStorage.setItem(tokenKey, token);
      localStorage.removeItem(usernameKey);

      // Clean up URL
      url.searchParams.delete("authToken");
      window.history.replaceState(window.history.state, "", url);

      this.currentUser = {
        username: "user",
        token,
      };
      return;
    }

    // Try to load from localStorage
    const storedToken = localStorage.getItem(tokenKey);
    const storedUsername = localStorage.getItem(usernameKey);

    if (storedToken) {
      this.currentUser = {
        username: storedUsername || "user",
        token: storedToken,
      };
      return;
    }

    // Fall back to mock mode if enabled
    if (mockMode) {
      const mockToken = "mock-token";
      const mockUsername = "demo";
      localStorage.setItem(tokenKey, mockToken);
      localStorage.setItem(usernameKey, mockUsername);
      this.currentUser = {
        username: mockUsername,
        token: mockToken,
      };
      return;
    }

    // Fall back to default account if configured
    // This allows the app to work without login in development or with a default account
    if (this.config.defaultAccount) {
      const {username, token} = this.config.defaultAccount;
      this.currentUser = {
        username,
        token,
      };
      // Save default account to storage so it persists
      localStorage.setItem(tokenKey, token);
      localStorage.setItem(usernameKey, username);
      return;
    }

    // No authentication found and no default account - redirect to login
    // Only redirect if we're not already on the login page
    if (window.location.pathname !== this.config.loginPath) {
      url.pathname = this.config.loginPath;
      window.history.replaceState(null, "", url);
    }
  }

  /**
   * Check if a user is currently authenticated
   */
  isAuthenticated(): boolean {
    return this.currentUser !== null;
  }

  /**
   * Get the current authenticated user
   */
  getCurrentUser(): AuthUser | null {
    return this.currentUser;
  }

  /**
   * Get the current username
   */
  getCurrentUsername(): string | null {
    return this.currentUser?.username ?? null;
  }

  /**
   * Get the current auth token
   */
  getCurrentToken(): string | null {
    return this.currentUser?.token ?? null;
  }

  /**
   * Login with username and password
   */
  async login(username: string, password: string): Promise<void> {
    if (!this.loginFunction) {
      throw new Error(
        "Login function not set. Call setLoginFunction() first."
      );
    }

    try {
      const token = await this.loginFunction(
        this.config.serverUrl,
        username,
        password
      );

      // Save to storage
      const {token: tokenKey, username: usernameKey} = this.config.storageKeys;
      localStorage.setItem(tokenKey, token);
      localStorage.setItem(usernameKey, username);

      // Update current user
      this.currentUser = {
        username,
        token,
      };

      // Redirect to main app
      window.location.replace(this.config.redirectAfterLogin);
    } catch (error) {
      // Re-throw to let caller handle
      throw error;
    }
  }

  /**
   * Logout the current user
   */
  logout(): void {
    const {token: tokenKey, username: usernameKey} = this.config.storageKeys;
    localStorage.removeItem(tokenKey);
    localStorage.removeItem(usernameKey);
    this.currentUser = null;

    // Redirect to login if not already there
    if (window.location.pathname !== this.config.loginPath) {
      window.location.assign(this.config.loginPath);
    }
  }

  /**
   * Set authentication token (useful for programmatic login)
   */
  setAuthToken(username: string, token: string): void {
    const {token: tokenKey, username: usernameKey} = this.config.storageKeys;
    localStorage.setItem(tokenKey, token);
    localStorage.setItem(usernameKey, username);
    this.currentUser = {
      username,
      token,
    };
    window.location.replace(this.config.redirectAfterLogin);
  }

  /**
   * Clear authentication token
   */
  clearAuthToken(): void {
    this.logout();
  }

  /**
   * Get an AuthProvider instance for use with Connection/InstanceState
   */
  getAuthProvider(): AuthProvider {
    return {
      getAuthToken: () => {
        const token = this.getCurrentToken();
        if (!token) {
          throw new Error("No authentication token available");
        }
        return token;
      },
      getAuthUser: () => {
        return this.getCurrentUsername() ?? undefined;
      },
      getUserRole: () => {
        return this.currentUser?.role ?? null;
      },
      invalidateToken: () => {
        this.logout();
      },
    };
  }

  /**
   * Set the user's role (typically fetched from server after login)
   */
  setUserRole(role: Role): void {
    if (this.currentUser) {
      this.currentUser.role = role;
    }
  }
}

// Default instance - can be configured or replaced
export const auth = new AuthManager({
  defaultAccount: mockMode
    ? {
        username: "demo",
        token: "mock-token",
      }
    : undefined,
});
