import {createContext, Model, model, prop} from "mobx-keystone";

import {InstanceState} from "@dbsof/studio/state/instance";
import {auth} from "@dbsof/auth";

export const serverUrl = "http://localhost:5757";

// Re-export auth functions for backward compatibility
export function setAuthToken(username: string, token: string) {
  auth.setAuthToken(username, token);
}

export function clearAuthToken() {
  auth.clearAuthToken();
}

export const appCtx = createContext<App>();

@model("App")
export class App extends Model({
  instanceState: prop(
    () =>
      new InstanceState({
        serverUrl,
      })
  ),
}) {
  onInit() {
    // Use the centralized auth module
    this.instanceState._authProvider = auth.getAuthProvider();

    // Fetch instance info if authenticated
    if (auth.isAuthenticated()) {
      this.instanceState.fetchInstanceInfo();
    }
    appCtx.set(this, this);
  }
}
