import {registerRootStore} from "mobx-keystone";
import {App} from "./models/app";
// Initialize auth module before creating app state
import "@dbsof/auth";

let appState: App | null = null;

appState = new App({});

registerRootStore(appState);

export default appState!;
