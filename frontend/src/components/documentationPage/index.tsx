import {Link} from "react-router-dom";
import CodeBlock from "@dbsof/common/ui/codeBlock";
import {CustomScrollbars} from "@dbsof/common/ui/customScrollbar";
import styles from "./documentationPage.module.scss";

export default function DocumentationPage() {
  return (
    <CustomScrollbars className={styles.documentationPage} innerClass={styles.scrollableContent}>
      <div className={styles.scrollableContent}>
        <div className={styles.contentInner}>
          <h1 className={styles.title}>Dbsof Studio Documentation</h1>

        <nav className={styles.tableOfContents}>
          <h2>Table of Contents</h2>
          <ul>
            <li><a href="#introduction">Introduction</a></li>
            <li><a href="#authentication">Authentication System</a></li>
            <li><a href="#branch-system">Branch System Architecture</a></li>
            <li><a href="#tabs">Tabs Documentation</a>
              <ul>
                <li><a href="#tab-dashboard">Dashboard</a></li>
                <li><a href="#tab-repl">REPL</a></li>
                <li><a href="#tab-editor">Query Editor</a></li>
                <li><a href="#tab-schema">Schema</a></li>
                <li><a href="#tab-dataview">Data Explorer</a></li>
                <li><a href="#tab-auth">Auth Admin</a></li>
                <li><a href="#tab-ai">AI</a></li>
                <li><a href="#tab-perf">Performance Stats</a></li>
              </ul>
            </li>
            <li><a href="#architecture">Code Architecture & Abstraction</a></li>
          </ul>
        </nav>

        <section id="introduction" className={styles.section}>
          <h2>Introduction</h2>
          <p>
            Dbsof Studio is a comprehensive database management and development environment
            built for modern database workflows. It provides an intuitive interface for
            managing database instances, branches, schemas, and data, along with powerful
            tools for querying, analyzing, and developing database applications.
          </p>
          <p>
            This documentation covers all aspects of the Studio, from authentication and
            branch management to detailed explanations of each tab and the underlying
            architecture that makes it all work together.
          </p>
        </section>

        <section id="authentication" className={styles.section}>
          <h2>Authentication System</h2>
          
          <h3>Overview</h3>
          <p>
            The authentication system in Dbsof Studio is centralized through the
            <code className={styles.inlineCode}>@dbsof/auth</code> package, which provides
            a unified <code className={styles.inlineCode}>AuthManager</code> class for
            handling all authentication-related operations.
          </p>

          <h3>AuthManager Class</h3>
          <p>
            The <code className={styles.inlineCode}>AuthManager</code> is the core of the
            authentication system. It handles:
          </p>
          <ul>
            <li>Token storage and retrieval from localStorage</li>
            <li>User session management</li>
            <li>Login and logout flows</li>
            <li>Default account fallback for development/mock mode</li>
            <li>AuthProvider generation for database connections</li>
          </ul>

          <h3>Token Storage</h3>
          <p>
            Authentication tokens are stored in browser localStorage using configurable keys:
          </p>
          <ul>
            <li><code className={styles.inlineCode}>dbsofAuthToken</code> - Stores the authentication token</li>
            <li><code className={styles.inlineCode}>dbsofAuthUsername</code> - Stores the username</li>
          </ul>
          <p>
            These keys can be customized when creating an <code className={styles.inlineCode}>AuthManager</code> instance.
          </p>

          <h3>HTTP SCRAM Authentication</h3>
          <p>
            The Studio uses HTTP SCRAM (Salted Challenge Response Authentication Mechanism)
            for secure password-based authentication. The login flow works as follows:
          </p>
          <ol>
            <li>User enters username and password in the login form</li>
            <li>The <code className={styles.inlineCode}>AuthManager.login()</code> method is called</li>
            <li>A login function (typically <code className={styles.inlineCode}>getHTTPSCRAMAuth()</code>) performs the SCRAM challenge-response</li>
            <li>Upon successful authentication, a token is received and stored</li>
            <li>The user is redirected to the main application</li>
          </ol>

          <h3>Default Account & Mock Mode</h3>
          <p>
            For development and testing, the Studio supports a default account fallback.
            When <code className={styles.inlineCode}>mockMode</code> is enabled or a default
            account is configured, users can access the Studio without explicit login:
          </p>
          <CodeBlock code={`// Default account configuration
const auth = new AuthManager({
  defaultAccount: {
    username: "demo",
    token: "mock-token",
  },
});`} />

          <h3>AuthProvider Interface</h3>
          <p>
            The <code className={styles.inlineCode}>AuthProvider</code> interface is used
            by database connections to authenticate requests. It provides:
          </p>
          <ul>
            <li><code className={styles.inlineCode}>getAuthToken()</code> - Returns the current auth token</li>
            <li><code className={styles.inlineCode}>getAuthUser()</code> - Returns the current username</li>
            <li><code className={styles.inlineCode}>getUserRole()</code> - Returns the user's role (if available)</li>
            <li><code className={styles.inlineCode}>invalidateToken()</code> - Logs out the user and clears the token</li>
          </ul>

          <h3>Login Flow</h3>
          <p>
            The complete login flow involves several steps:
          </p>
          <ol>
            <li>User navigates to <code className={styles.inlineCode}>/ui/_login</code></li>
            <li>Login form is displayed (<code className={styles.inlineCode}>LoginPage</code> component)</li>
            <li>User submits credentials</li>
            <li><code className={styles.inlineCode}>auth.setLoginFunction()</code> must be called first to set the SCRAM auth function</li>
            <li><code className={styles.inlineCode}>auth.login()</code> performs authentication</li>
            <li>Token is stored in localStorage</li>
            <li>User is redirected to <code className={styles.inlineCode}>/ui</code></li>
            <li>The <code className={styles.inlineCode}>App</code> model initializes with the auth provider</li>
          </ol>

          <h3>Logout Flow</h3>
          <p>
            Logging out clears all authentication state:
          </p>
          <ol>
            <li><code className={styles.inlineCode}>auth.logout()</code> is called</li>
            <li>Token and username are removed from localStorage</li>
            <li>Current user state is cleared</li>
            <li>User is redirected to the login page</li>
          </ol>

          <h3>URL Token Handling</h3>
          <p>
            The authentication system also supports token-based login via URL parameters.
            If an <code className={styles.inlineCode}>authToken</code> query parameter is present:
          </p>
          <ul>
            <li>The token is extracted and stored in localStorage</li>
            <li>The query parameter is removed from the URL</li>
            <li>The user is automatically authenticated</li>
          </ul>
          <p>
            This is useful for OAuth callbacks and programmatic authentication flows.
          </p>
        </section>

        <section id="branch-system" className={styles.section}>
          <h2>Branch System Architecture</h2>

          <h3>Hierarchical Structure</h3>
          <p>
            The Dbsof Studio uses a hierarchical structure for organizing data:
          </p>
          <div className={styles.diagram}>
            <pre>{`Users
  └── Instances (e.g., "Demo Instance")
      └── Branches (e.g., "main", "feature-branch", "_example")`}</pre>
          </div>

          <h3>Users</h3>
          <p>
            Users are authenticated individuals who can access the Studio. Each user can:
          </p>
          <ul>
            <li>Be part of multiple instances</li>
            <li>Have different roles and permissions per instance</li>
            <li>Access branches within instances they have permission for</li>
          </ul>

          <h3>Instances</h3>
          <p>
            An instance represents a database server or deployment. Each instance:
          </p>
          <ul>
            <li>Has a unique <code className={styles.inlineCode}>instanceId</code> (e.g., "demo", "production")</li>
            <li>Has a display name (e.g., "Demo Instance")</li>
            <li>Contains multiple branches (databases)</li>
            <li>Has its own server URL and configuration</li>
            <li>Is scoped by the authenticated user's permissions</li>
          </ul>
          <p>
            The instance information is fetched from the server when the app initializes,
            and includes metadata such as available branches, server version, and capabilities.
          </p>

          <h3>Branches</h3>
          <p>
            Branches (also referred to as databases) are isolated database environments
            within an instance. Each branch:
          </p>
          <ul>
            <li>Has a unique name within the instance</li>
            <li>Can be created from another branch (branching)</li>
            <li>Can optionally copy data from the source branch</li>
            <li>Has its own schema and data</li>
            <li>Maintains a migration history</li>
            <li>Can be visualized in a branch graph showing relationships</li>
          </ul>

          <h3>Creating Branches</h3>
          <p>
            Branches can be created in several ways:
          </p>
          <ol>
            <li><strong>Empty branch:</strong> Created without a source, starts with an empty schema</li>
            <li><strong>From existing branch:</strong> Created from another branch, inheriting its schema</li>
            <li><strong>With data copy:</strong> Optionally copies data from the source branch</li>
          </ol>
          <p>
            The creation process involves:
          </p>
          <CodeBlock code={`POST /instances/{instanceId}/databases
{
  "name": "new-branch",
  "fromBranch": "main",  // optional
  "copyData": false       // optional
}`} />

          <h3>Branch Graph</h3>
          <p>
            The branch graph visualization shows:
          </p>
          <ul>
            <li>All branches in the instance</li>
            <li>Parent-child relationships between branches</li>
            <li>Migration history and lineage</li>
            <li>Branch status and metadata</li>
          </ul>
          <p>
            The graph is interactive, allowing users to:
          </p>
          <ul>
            <li>Navigate to branches by clicking on them</li>
            <li>View migration details</li>
            <li>See branch creation timestamps</li>
            <li>Understand the branching structure visually</li>
          </ul>

          <h3>Data Isolation</h3>
          <p>
            Each branch maintains complete data isolation:
          </p>
          <ul>
            <li>Schema changes in one branch do not affect others</li>
            <li>Data is separate unless explicitly copied during branch creation</li>
            <li>Migrations are tracked per branch</li>
            <li>Queries and operations are scoped to the current branch</li>
          </ul>

          <h3>Migration System</h3>
          <p>
            Branches track their migration history:
          </p>
          <ul>
            <li>Each migration has a unique ID</li>
            <li>Migrations are ordered chronologically</li>
            <li>The migration history shows the evolution of the schema</li>
            <li>Branches created from others inherit the parent's migration history up to the creation point</li>
          </ul>

          <h3>Instance Scoping</h3>
          <p>
            All operations are scoped to the current instance:
          </p>
          <ul>
            <li>API requests include the instance ID in the path</li>
            <li>Branch names are unique within an instance</li>
            <li>User permissions are checked at the instance level</li>
            <li>The instance context is provided through React context and MobX-Keystone</li>
          </ul>
          <p>
            The default instance ID is "demo" in mock mode, but can be configured per deployment.
          </p>
        </section>

        <section id="tabs" className={styles.section}>
          <h2>Tabs Documentation</h2>
          <p>
            The Studio provides eight main tabs, each serving a specific purpose in the
            database development workflow. Tabs are accessible from the vertical sidebar
            on the left when viewing a branch.
          </p>

          <section id="tab-dashboard" className={styles.subsection}>
            <h3>Dashboard</h3>
            <p>
              The Dashboard tab provides an overview of the current branch and quick access
              to common actions.
            </p>
            <h4>Features</h4>
            <ul>
              <li><strong>Branch Overview:</strong> Displays the branch name and status</li>
              <li><strong>Quick Actions:</strong> Buttons to quickly navigate to REPL, Editor, Schema Viewer, and Data Viewer</li>
              <li><strong>Statistics:</strong> Shows object count and object type count</li>
              <li><strong>First Run Experience:</strong> Special UI for empty branches with option to create example branch</li>
            </ul>
            <h4>Usage</h4>
            <p>
              The Dashboard is the default tab when opening a branch. It serves as a
              starting point for database exploration and provides shortcuts to other
              functionality. For empty branches, it offers a guided experience to create
              an example branch with sample data.
            </p>
          </section>

          <section id="tab-repl" className={styles.subsection}>
            <h3>REPL (Read-Eval-Print Loop)</h3>
            <p>
              The REPL provides an interactive EdgeQL/SQL shell for executing queries and
              commands directly against the database.
            </p>
            <h4>Features</h4>
              <ul>
                <li><strong>Interactive Query Execution:</strong> Type queries and execute with Ctrl+Enter or Enter (for complete statements)</li>
                <li><strong>Command History:</strong> Navigate through previous queries with Ctrl+ArrowUp/Down</li>
                <li><strong>Dual Language Support:</strong> Switch between EdgeQL (default) and SQL modes</li>
                <li><strong>Result Display:</strong> View results in tree or table format</li>
                <li><strong>Query Explanations:</strong> Use EXPLAIN to see query execution plans</li>
                <li><strong>Command System:</strong> Built-in commands like <code className={styles.inlineCode}>\help</code>, <code className={styles.inlineCode}>\clear</code></li>
                <li><strong>Retro Mode:</strong> Optional terminal-style appearance</li>
              </ul>
            <h4>Keyboard Shortcuts</h4>
            <ul>
              <li><code className={styles.inlineCode}>Ctrl+Enter</code> or <code className={styles.inlineCode}>Enter</code> (end of statement) - Execute query</li>
              <li><code className={styles.inlineCode}>Ctrl+ArrowUp</code> - Previous query in history</li>
              <li><code className={styles.inlineCode}>Ctrl+ArrowDown</code> - Next query in history</li>
              <li><code className={styles.inlineCode}>Ctrl+C</code> - Cancel running query</li>
            </ul>
            <h4>Result Modes</h4>
            <ul>
              <li><strong>Tree View:</strong> Hierarchical display of nested objects and arrays</li>
              <li><strong>Table View:</strong> Tabular grid display for flat result sets</li>
            </ul>
            <h4>Usage</h4>
            <p>
              The REPL is ideal for quick queries, testing, and exploration. It maintains
              a persistent history that persists across sessions. Results can be expanded
              to view full details, and queries can be sent to the Query Editor for more
              advanced editing.
            </p>
          </section>

          <section id="tab-editor" className={styles.subsection}>
            <h3>Query Editor</h3>
            <p>
              The Query Editor provides a full-featured code editor for writing and
              executing complex queries with parameter support and result analysis.
            </p>
            <h4>Features</h4>
            <ul>
              <li><strong>Advanced Code Editor:</strong> Syntax highlighting, autocomplete, and code formatting</li>
              <li><strong>Query Parameters:</strong> Define and bind parameters for parameterized queries</li>
              <li><strong>Query History:</strong> Access to previously executed queries</li>
              <li><strong>Result Visualization:</strong> Multiple output modes (tree, table, JSON)</li>
              <li><strong>Query Explanation:</strong> Visual explain plans showing query execution</li>
              <li><strong>Split View:</strong> Side-by-side query and results</li>
              <li><strong>Thumbnail Generation:</strong> Visual previews of query results</li>
            </ul>
            <h4>Parameter Binding</h4>
            <p>
              The Query Editor supports parameterized queries using EdgeQL syntax:
            </p>
            <CodeBlock code={`SELECT User {
  name,
  email
} FILTER .id = <uuid>$userId`} />
            <p>
              Parameters are defined in a dedicated panel and can be bound before execution.
            </p>
            <h4>Query History</h4>
            <p>
              All executed queries are saved to history, allowing you to:
            </p>
            <ul>
              <li>Re-run previous queries</li>
              <li>Load queries into the editor</li>
              <li>View execution times and results</li>
            </ul>
            <h4>Usage</h4>
            <p>
              Use the Query Editor for complex queries, parameterized operations, and
              when you need advanced editing features. It's the primary tool for
              developing and refining database queries.
            </p>
          </section>

          <section id="tab-schema" className={styles.subsection}>
            <h3>Schema</h3>
            <p>
              The Schema tab provides comprehensive tools for exploring and understanding
              the database schema structure.
            </p>
            <h4>Features</h4>
            <ul>
              <li><strong>Graph View:</strong> Interactive visual representation of schema relationships</li>
              <li><strong>Text View:</strong> Textual schema definition in EdgeQL SDL format</li>
              <li><strong>Type Exploration:</strong> Navigate through object types, scalars, functions, and more</li>
              <li><strong>Relationship Visualization:</strong> See links, properties, and inheritance</li>
              <li><strong>Label Toggle:</strong> Show/hide labels in graph view for clarity</li>
            </ul>
            <h4>Graph View</h4>
            <p>
              The graph view displays:
            </p>
            <ul>
              <li>Object types as nodes</li>
              <li>Links and properties as edges</li>
              <li>Inheritance relationships</li>
              <li>Function signatures</li>
              <li>Constraints and annotations</li>
            </ul>
            <p>
              The graph is interactive - you can pan, zoom, and click on nodes to see details.
            </p>
            <h4>Text View</h4>
            <p>
              The text view shows the complete schema definition in EdgeQL Schema Definition
              Language (SDL) format, making it easy to:
            </p>
            <ul>
              <li>Read the complete schema</li>
              <li>Copy schema definitions</li>
              <li>Understand type definitions</li>
            </ul>
            <h4>Usage</h4>
            <p>
              Use the Schema tab to understand your database structure, explore relationships
              between types, and get familiar with the data model. The graph view is
              particularly useful for visualizing complex schemas with many relationships.
            </p>
          </section>

          <section id="tab-dataview" className={styles.subsection}>
            <h3>Data Explorer</h3>
            <p>
              The Data Explorer (also called Data Viewer) allows you to browse, filter,
              and edit data in your database.
            </p>
            <h4>Features</h4>
            <ul>
              <li><strong>Object Type Selection:</strong> Choose which object type to explore</li>
              <li><strong>Data Inspection:</strong> Navigate through objects and their properties</li>
              <li><strong>Filtering:</strong> Apply filters to narrow down results</li>
              <li><strong>Data Editing:</strong> Modify object properties and links</li>
              <li><strong>Review Workflow:</strong> Review changes before committing</li>
              <li><strong>Breadcrumb Navigation:</strong> Navigate through nested objects</li>
            </ul>
            <h4>Inspector Stack</h4>
            <p>
              The Data Explorer uses an inspector stack pattern:
            </p>
            <ul>
              <li>Each level shows objects of a selected type</li>
              <li>Clicking on an object pushes a new inspector level</li>
              <li>Breadcrumbs show your navigation path</li>
              <li>Back button returns to previous level</li>
            </ul>
            <h4>Filtering</h4>
            <p>
              Filters can be applied to narrow down the displayed objects:
            </p>
            <ul>
              <li>Filter by property values</li>
              <li>Filter by link relationships</li>
              <li>Combine multiple filters</li>
            </ul>
            <h4>Editing</h4>
            <p>
              The Data Explorer supports editing:
            </p>
            <ul>
              <li>Modify scalar properties</li>
              <li>Add/remove links</li>
              <li>Create new objects</li>
              <li>Delete objects</li>
            </ul>
            <p>
              Changes are staged and can be reviewed before committing to the database.
            </p>
            <h4>Usage</h4>
            <p>
              Use the Data Explorer to browse your data, understand relationships, and
              make quick edits. It's particularly useful for data exploration and
              manual data corrections.
            </p>
          </section>

          <section id="tab-auth" className={styles.subsection}>
            <h3>Auth Admin</h3>
            <p>
              The Auth Admin tab provides comprehensive management of the authentication
              extension, including user management, providers, and configuration.
            </p>
            <h4>Prerequisites</h4>
            <p>
              The Auth Admin tab requires:
            </p>
            <ul>
              <li>The <code className={styles.inlineCode}>auth</code> extension to be enabled in the schema</li>
              <li>Permission: <code className={styles.inlineCode}>sys::perm::branch_config</code> and <code className={styles.inlineCode}>ext::auth::perm::auth_read</code></li>
            </ul>
            <h4>Features</h4>
            <ul>
              <li><strong>User Management:</strong> View, create, edit, and delete users</li>
              <li><strong>Authentication Providers:</strong> Configure OAuth, email/password, and other providers</li>
              <li><strong>SMTP Configuration:</strong> Set up email sending for password reset and verification</li>
              <li><strong>Webhooks:</strong> Configure webhooks for auth events</li>
              <li><strong>Login UI Preview:</strong> Preview and customize the login UI</li>
              <li><strong>Provider Testing:</strong> Test authentication providers</li>
            </ul>
            <h4>User Management</h4>
            <p>
              The user management interface allows you to:
            </p>
            <ul>
              <li>View all users in the system</li>
              <li>See user roles and permissions</li>
              <li>Create new users</li>
              <li>Edit user properties</li>
              <li>Reset passwords</li>
              <li>Enable/disable users</li>
            </ul>
            <h4>Providers</h4>
            <p>
              Authentication providers can be configured for:
            </p>
            <ul>
              <li>OAuth providers (Google, GitHub, etc.)</li>
              <li>Email/password authentication</li>
              <li>Magic link authentication</li>
              <li>Custom providers</li>
            </ul>
            <h4>SMTP Configuration</h4>
            <p>
              SMTP settings are used for:
            </p>
            <ul>
              <li>Password reset emails</li>
              <li>Email verification</li>
              <li>Welcome emails</li>
            </ul>
            <h4>Usage</h4>
            <p>
              Use the Auth Admin tab to configure authentication for your application.
              This is essential for applications that require user authentication and
              authorization.
            </p>
          </section>

          <section id="tab-ai" className={styles.subsection}>
            <h3>AI</h3>
            <p>
              The AI tab provides tools for managing AI programs, prompts, and providers
              integrated with your database.
            </p>
            <h4>Features</h4>
            <ul>
              <li><strong>AI Programs:</strong> Create and manage AI programs that interact with your database</li>
              <li><strong>Program Graph:</strong> Visual representation of program flow and dependencies</li>
              <li><strong>Prompts:</strong> Manage prompts used by AI programs</li>
              <li><strong>Providers:</strong> Configure AI service providers (OpenAI, Anthropic, etc.)</li>
              <li><strong>RAG System:</strong> Configure Retrieval-Augmented Generation for context-aware AI</li>
              <li><strong>Program Execution:</strong> Run and test AI programs</li>
            </ul>
            <h4>AI Programs</h4>
            <p>
              AI programs are workflows that combine:
            </p>
            <ul>
              <li>Database queries</li>
              <li>AI model calls</li>
              <li>Data processing</li>
              <li>Response generation</li>
            </ul>
            <p>
              Programs are defined as graphs showing the flow of data and operations.
            </p>
            <h4>Program Graph</h4>
            <p>
              The program graph visualization shows:
            </p>
            <ul>
              <li>Program nodes (queries, AI calls, processing steps)</li>
              <li>Edges showing data flow</li>
              <li>Program status and execution state</li>
              <li>Dependencies between components</li>
            </ul>
            <h4>RAG System</h4>
            <p>
              The RAG (Retrieval-Augmented Generation) system allows AI programs to:
            </p>
            <ul>
              <li>Retrieve relevant context from the database</li>
              <li>Use that context to generate more accurate responses</li>
              <li>Maintain conversation context</li>
              <li>Learn from database structure and content</li>
            </ul>
            <h4>Usage</h4>
            <p>
              Use the AI tab to build intelligent applications that leverage AI capabilities
              while maintaining a strong connection to your database. This is useful for
              chatbots, intelligent assistants, and AI-powered features.
            </p>
          </section>

          <section id="tab-perf" className={styles.subsection}>
            <h3>Performance Stats</h3>
            <p>
              The Performance Stats tab provides insights into query performance and
              database statistics.
            </p>
            <h4>Features</h4>
            <ul>
              <li><strong>Query Performance Analysis:</strong> View execution times and statistics for queries</li>
              <li><strong>Performance Charts:</strong> Visualize performance trends over time</li>
              <li><strong>Statistics Tables:</strong> Detailed breakdown of query performance</li>
              <li><strong>Filtering:</strong> Filter stats by time range, query type, and other criteria</li>
              <li><strong>Import Jobs:</strong> Monitor and manage data import operations</li>
            </ul>
            <h4>Query Analysis</h4>
            <p>
              The performance stats show:
            </p>
            <ul>
              <li>Query execution times</li>
              <li>Number of queries executed</li>
              <li>Slow query identification</li>
              <li>Query patterns and frequency</li>
            </ul>
            <h4>Import Jobs</h4>
            <p>
              Import jobs allow you to:
            </p>
            <ul>
              <li>Upload CSV files</li>
              <li>Import data into the database</li>
              <li>Monitor import progress</li>
              <li>View import history</li>
              <li>Track import statistics (rows imported, files processed)</li>
            </ul>
            <h4>Usage</h4>
            <p>
              Use the Performance Stats tab to monitor database performance, identify
              slow queries, and track data import operations. This is essential for
              maintaining optimal database performance.
            </p>
          </section>
        </section>

        <section id="architecture" className={styles.section}>
          <h2>Code Architecture & Abstraction</h2>

          <h3>Workspace Package Structure</h3>
          <p>
            The Dbsof Studio codebase is organized as a monorepo with multiple workspace
            packages, each serving a specific purpose:
          </p>

          <h4>@dbsof/auth</h4>
          <p>
            Centralized authentication management:
          </p>
          <ul>
            <li><code className={styles.inlineCode}>AuthManager</code> class for authentication</li>
            <li>Token storage and session management</li>
            <li>AuthProvider interface implementation</li>
            <li>Default account and mock mode support</li>
          </ul>

          <h4>@dbsof/common</h4>
          <p>
            Shared component library and utilities used across the application:
          </p>
          <ul>
            <li><strong>UI Components:</strong> Reusable React components (buttons, inputs, modals, etc.)</li>
            <li><strong>Hooks:</strong> Custom React hooks (useTheme, useModal, useTooltips, etc.)</li>
            <li><strong>Utils:</strong> Utility functions and helpers</li>
            <li><strong>Branch Graph:</strong> Branch visualization component</li>
            <li><strong>Schema Data:</strong> Schema type definitions and utilities</li>
            <li><strong>Code Block:</strong> Syntax-highlighted code display</li>
          </ul>

          <h4>@dbsof/studio</h4>
          <p>
            Studio-specific components and state management:
          </p>
          <ul>
            <li><strong>Tabs:</strong> All tab implementations (Dashboard, REPL, Editor, etc.)</li>
            <li><strong>State:</strong> MobX-Keystone models (InstanceState, DatabaseState, tab states)</li>
            <li><strong>Components:</strong> Studio-specific UI components</li>
            <li><strong>Icons:</strong> Custom icon components</li>
            <li><strong>Hooks:</strong> Database routing and context hooks</li>
          </ul>

          <h4>@dbsof/platform</h4>
          <p>
            Platform client and API utilities:
          </p>
          <ul>
            <li>HTTP client for API communication</li>
            <li>SCRAM authentication implementation</li>
            <li>Connection management</li>
            <li>Mock mode configuration</li>
          </ul>

          <h4>@dbsof/code-editor</h4>
          <p>
            Code editor component based on CodeMirror:
          </p>
          <ul>
            <li>Syntax highlighting</li>
            <li>Autocomplete</li>
            <li>Code formatting</li>
            <li>Custom extensions</li>
          </ul>

          <h4>@dbsof/inspector</h4>
          <p>
            Data inspector component for displaying structured data:
          </p>
          <ul>
            <li>Tree view of nested objects</li>
            <li>Expandable/collapsible nodes</li>
            <li>Keyboard navigation</li>
            <li>Virtual scrolling for performance</li>
          </ul>

          <h4>@dbsof/node-graph</h4>
          <p>
            Graph visualization library:
          </p>
          <ul>
            <li>Schema graph rendering</li>
            <li>Interactive node manipulation</li>
            <li>Zoom and pan controls</li>
            <li>Minimap support</li>
          </ul>

          <h3>State Management with MobX-Keystone</h3>
          <p>
            The Studio uses MobX-Keystone for state management, providing:
          </p>
          <ul>
            <li><strong>Reactive Models:</strong> Observable state that automatically updates UI</li>
            <li><strong>Model Classes:</strong> Type-safe model definitions with TypeScript</li>
            <li><strong>Actions & Flows:</strong> Async operations and state mutations</li>
            <li><strong>Context:</strong> Dependency injection for models</li>
            <li><strong>Persistence:</strong> Optional persistence to IndexedDB</li>
          </ul>
          <p>
            Key models include:
          </p>
          <ul>
            <li><code className={styles.inlineCode}>App</code> - Root application model</li>
            <li><code className={styles.inlineCode}>InstanceState</code> - Instance-level state</li>
            <li><code className={styles.inlineCode}>DatabaseState</code> - Branch-level state</li>
            <li><code className={styles.inlineCode}>SessionState</code> - Session-scoped state</li>
            <li>Tab-specific state models (Repl, QueryEditor, Schema, etc.)</li>
          </ul>

          <h3>Theme System and CSS Variables</h3>
          <p>
            The Studio uses a centralized theme system with CSS variables:
          </p>
          <ul>
            <li><strong>Light & Dark Themes:</strong> Automatic theme switching</li>
            <li><strong>CSS Variables:</strong> Centralized color and styling definitions</li>
            <li><strong>Semantic Colors:</strong> Variables like <code className={styles.inlineCode}>--app-bg</code>, <code className={styles.inlineCode}>--app-text-colour</code></li>
            <li><strong>Component Theming:</strong> Components automatically adapt to theme</li>
          </ul>
          <p>
            All colors are defined in <code className={styles.inlineCode}>app.module.scss</code> and
            components use these variables instead of hardcoded colors, ensuring consistent
            theming across the application.
          </p>

          <h3>Code Reuse Patterns</h3>
          <p>
            The codebase emphasizes code reuse through:
          </p>
          <ul>
            <li><strong>Shared Components:</strong> Common UI components in <code className={styles.inlineCode}>@dbsof/common</code></li>
            <li><strong>Custom Hooks:</strong> Reusable logic extracted into hooks</li>
            <li><strong>Utility Functions:</strong> Shared utilities for common operations</li>
            <li><strong>Type Definitions:</strong> Shared TypeScript types and interfaces</li>
            <li><strong>Abstractions:</strong> Higher-level abstractions for complex operations</li>
          </ul>

          <h3>Component Abstraction Principles</h3>
          <p>
            Components are designed with abstraction in mind:
          </p>
          <ul>
            <li><strong>Composition:</strong> Complex components built from simpler ones</li>
            <li><strong>Props Interfaces:</strong> Clear, well-defined prop types</li>
            <li><strong>Separation of Concerns:</strong> UI components separate from business logic</li>
            <li><strong>State Management:</strong> State lifted to appropriate levels</li>
            <li><strong>Context Usage:</strong> Context for dependency injection and state sharing</li>
          </ul>

          <h3>Routing Architecture</h3>
          <p>
            The Studio uses React Router for navigation:
          </p>
          <ul>
            <li><strong>Instance Routes:</strong> <code className={styles.inlineCode}>/ui/</code> - Instance page</li>
            <li><strong>Branch Routes:</strong> <code className={styles.inlineCode}>/ui/:databaseName</code> - Branch dashboard</li>
            <li><strong>Tab Routes:</strong> <code className={styles.inlineCode}>/ui/:databaseName/:tab</code> - Specific tab</li>
            <li><strong>Nested Routes:</strong> Tabs can have nested routes for sub-features</li>
            <li><strong>Login Route:</strong> <code className={styles.inlineCode}>/ui/_login</code> - Login page</li>
          </ul>
          <p>
            The routing system includes:
          </p>
          <ul>
            <li>Database router context for branch-scoped navigation</li>
            <li>Tab specifications defining routes and components</li>
            <li>Lazy loading for tab components</li>
            <li>URL state synchronization</li>
          </ul>

          <h3>API Communication</h3>
          <p>
            The Studio communicates with the backend through:
          </p>
          <ul>
            <li><strong>REST API:</strong> Standard REST endpoints for CRUD operations</li>
            <li><strong>HTTP SCRAM:</strong> Secure authentication for API requests</li>
            <li><strong>Connection Management:</strong> Reusable connection objects with auth</li>
            <li><strong>Error Handling:</strong> Centralized error handling and display</li>
            <li><strong>Mock Mode:</strong> Development mode with mocked API responses</li>
          </ul>

          <h3>Performance Optimizations</h3>
          <p>
            The Studio includes several performance optimizations:
          </p>
          <ul>
            <li><strong>Virtual Scrolling:</strong> Large lists use virtual scrolling</li>
            <li><strong>Lazy Loading:</strong> Tab components loaded on demand</li>
            <li><strong>Code Splitting:</strong> Bundle splitting for faster initial load</li>
            <li><strong>Memoization:</strong> React.memo and useMemo for expensive computations</li>
            <li><strong>Debouncing:</strong> Input debouncing for search and filters</li>
            <li><strong>Caching:</strong> Schema data and query results cached</li>
          </ul>
        </section>

        <footer className={styles.footer}>
          <p>
            For API documentation, see the <code className={styles.inlineCode}>openapi.yaml</code> file in the project root.
          </p>
          <p>
            <Link to="/">← Back to Instance</Link>
          </p>
        </footer>
        </div>
      </div>
    </CustomScrollbars>
  );
}
