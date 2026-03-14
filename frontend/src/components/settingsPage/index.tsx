import { useEffect, useState } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { getUserSettings, updateUserSettings, UserSettings } from "@dbsof/platform/userSettings";
import { CustomScrollbars } from "@dbsof/common/ui/customScrollbar";
import { Button, TextInput, Checkbox } from "@dbsof/common/newui";
import Spinner from "@dbsof/common/ui/spinner";
import styles from "./settingsPage.module.scss";

export function SettingsPage() {
  const { instanceId } = useParams<{ instanceId: string }>();
  const navigate = useNavigate();
  const [settings, setSettings] = useState<UserSettings>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [saveSuccess, setSaveSuccess] = useState(false);

  const serverUrl = "http://localhost:5757";
  const userId = "demo"; // For now, using demo user

  useEffect(() => {
    async function fetchSettings() {
      try {
        setLoading(true);
        setError(null);
        const fetchedSettings = await getUserSettings(serverUrl, userId);
        setSettings(fetchedSettings || {});
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load settings");
      } finally {
        setLoading(false);
      }
    }
    fetchSettings();
  }, []);

  const handleSave = async () => {
    try {
      setSaving(true);
      setSaveSuccess(false);
      setError(null);
      await updateUserSettings(serverUrl, userId, settings);
      setSaveSuccess(true);
      setTimeout(() => setSaveSuccess(false), 3000);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to save settings");
    } finally {
      setSaving(false);
    }
  };

  const handleChange = (key: string, value: any) => {
    setSettings({ ...settings, [key]: value });
  };

  if (loading) {
    return (
      <div className={styles.settingsPage}>
        <div className={styles.loading}>
          <Spinner size={20} />
          Loading settings...
        </div>
      </div>
    );
  }

  return (
    <CustomScrollbars className={styles.settingsPage} innerClass={styles.scrollableContent}>
      <div className={styles.scrollableContent}>
        <div className={styles.content}>
          <div className={styles.header}>
            <h1>User Settings</h1>
            <Button kind="outline" onClick={() => navigate(-1)}>
              ← Back
            </Button>
          </div>

          {error && <div className={styles.error}>{error}</div>}
          {saveSuccess && <div className={styles.success}>Settings saved successfully!</div>}

          <div className={styles.settingsForm}>
            {Object.keys(settings).length === 0 ? (
              <div className={styles.empty}>No settings configured yet.</div>
            ) : (
              Object.entries(settings).map(([key, value]) => (
                <div key={key} className={styles.settingRow}>
                  <label className={styles.settingLabel}>{key}</label>
                  <div className={styles.settingInput}>
                    {typeof value === "boolean" ? (
                      <Checkbox
                        label={value ? "Enabled" : "Disabled"}
                        checked={value}
                        onChange={(checked) => handleChange(key, checked)}
                      />
                    ) : typeof value === "number" ? (
                      <TextInput
                        type="text"
                        label=""
                        value={String(value)}
                        onChange={(e) => handleChange(key, Number(e.target.value))}
                      />
                    ) : value === null ? (
                      <TextInput
                        type="text"
                        label=""
                        value=""
                        placeholder="null"
                        onChange={(e) => handleChange(key, e.target.value || null)}
                      />
                    ) : (
                      <TextInput
                        type="text"
                        label=""
                        value={String(value)}
                        onChange={(e) => handleChange(key, e.target.value)}
                      />
                    )}
                  </div>
                </div>
              ))
            )}
          </div>

          <div className={styles.actions}>
            <Button
              kind="primary"
              onClick={handleSave}
              disabled={saving || Object.keys(settings).length === 0}
              loading={saving}
            >
              Save Settings
            </Button>
          </div>
        </div>
      </div>
    </CustomScrollbars>
  );
}
