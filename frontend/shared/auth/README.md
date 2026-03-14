# Authentication Module

Centralized authentication management for the Dbsof UI application.

## Overview

The `@dbsof/auth` module provides a single source of truth for authentication state and operations. It handles:

- Token storage and retrieval
- User session management
- Default account fallback
- Login/logout operations
- AuthProvider generation for use with Connection/InstanceState

## Usage

### Basic Usage

```typescript
import {auth} from "@dbsof/auth";

// Check if user is authenticated
if (auth.isAuthenticated()) {
  // User is logged in
}

// Get current user
const user = auth.getCurrentUser();
const username = auth.getCurrentUsername();
const token = auth.getCurrentToken();

// Login
await auth.login(username, password);

// Logout
auth.logout();

// Get AuthProvider for use with Connection/InstanceState
const authProvider = auth.getAuthProvider();
```

### Configuration

The auth module can be configured when creating a custom instance:

```typescript
import {AuthManager} from "@dbsof/auth";

const customAuth = new AuthManager({
  defaultAccount: {
    username: "admin",
    token: "default-token",
  },
  storageKeys: {
    token: "customTokenKey",
    username: "customUsernameKey",
  },
  loginPath: "/custom/login",
  redirectAfterLogin: "/custom/app",
  serverUrl: "https://api.example.com",
});
```

### Setting Login Function

Before using `auth.login()`, you must set the login function:

```typescript
import {auth} from "@dbsof/auth";
import {getHTTPSCRAMAuth} from "@dbsof/platform/client";

const httpSCRAMAuth = getHTTPSCRAMAuth();
auth.setLoginFunction(httpSCRAMAuth);
```

## Default Account

The auth module supports a default account that is used when:

1. No authentication token is found in localStorage
2. No token is provided via URL query params
3. Mock mode is disabled (or not applicable)

To enable a default account, configure it when creating the AuthManager:

```typescript
export const auth = new AuthManager({
  defaultAccount: {
    username: "admin",
    token: "your-default-token",
  },
});
```

If no default account is configured and no authentication is found, users will be redirected to the login page.

## Integration with Existing Code

The module maintains backward compatibility with existing code:

- `setAuthToken()` and `clearAuthToken()` functions are still exported from `app.ts` for backward compatibility
- The `AuthProvider` interface matches the existing pattern used by `Connection` and `InstanceState`

## Architecture

### AuthManager Class

The `AuthManager` class encapsulates all authentication logic:

- **State Management**: Tracks current user and token
- **Storage**: Handles localStorage operations
- **Initialization**: Checks URL params, localStorage, and default account on startup
- **Login/Logout**: Provides clean API for authentication operations
- **Provider Generation**: Creates `AuthProvider` instances for use with backend connections

### Initialization Flow

1. Check URL query params for `authToken` (for OAuth callbacks)
2. Check localStorage for stored token/username
3. Fall back to mock mode if enabled
4. Fall back to default account if configured
5. Redirect to login page if no authentication found

## Migration Notes

The following files have been updated to use the new auth module:

- `web/src/state/models/app.ts` - Uses `auth.getAuthProvider()`
- `web/src/components/loginPage/index.tsx` - Uses `auth.login()`
- `web/src/components/header/index.tsx` - Uses `auth.getCurrentUsername()` and `auth.logout()`

Old module-level variables (`authToken`, `authUsername`) have been removed in favor of the centralized `AuthManager` instance.
