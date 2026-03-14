import {useEffect, useState} from "react";
import {useForm} from "react-hook-form";

import {useModal} from "@dbsof/common/hooks/useModal";
import {
  Checkbox,
  Modal,
  ModalContent,
  Select,
  SubmitButton,
  TextInput,
} from "@dbsof/common/newui";
import {storeLocalStorageCacheItem} from "@dbsof/common/utils/localStorageCache";

import {InstanceState} from "../../state/instance";

import styles from "./modals.module.scss";

interface CreateBranchModalProps {
  legacy?: boolean;
  instanceState: InstanceState;
  fromBranch?: string;
  navigateToDB: (dbName: string) => void;
}

export default function CreateBranchModal({
  legacy,
  instanceState,
  fromBranch,
  navigateToDB,
}: CreateBranchModalProps) {
  const {openModal} = useModal();

  const [error, setError] = useState("");

  const {register, handleSubmit, formState, setFocus, watch, setValue} =
    useForm<{
      branchName: string;
      fromBranch: null | string;
      copyData: boolean;
    }>({
      defaultValues: {
        branchName: "",
        fromBranch: fromBranch ?? null,
        copyData: false,
      },
      mode: "onChange",
    });

  useEffect(() => {
    setFocus("branchName");
  }, []);

  const onSubmit = handleSubmit(async ({branchName, fromBranch, copyData}) => {
    setError("");
    try {
      const instanceId = instanceState.instanceId ?? "demo";
      const url = `${instanceState.serverUrl}/instances/${encodeURIComponent(instanceId)}/databases`;
      
      const response = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          name: branchName,
          fromBranch: fromBranch || null,
          copyData: copyData,
        }),
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({error: "Failed to create database"}));
        throw new Error(errorData.error || `HTTP ${response.status}: Failed to create database`);
      }

      await instanceState.fetchInstanceInfo();
      // Clear branch graph cache to force refresh
      storeLocalStorageCacheItem("branches-graph", instanceState.instanceId ?? "demo", null);
      navigateToDB(branchName);
      openModal(null);
    } catch (e) {
      setError((e as any).message || (e as any).toString());
    }
  });

  return (
    <Modal
      title={legacy ? "New Database" : "New Branch"}
      onClose={() => openModal(null, true)}
      onSubmit={onSubmit}
      formError={error}
      footerButtons={
        <SubmitButton
          kind="primary"
          loading={formState.isSubmitting}
          disabled={!formState.isValid}
        >
          {legacy ? "Create database" : "Create branch"}
        </SubmitButton>
      }
    >
      <ModalContent className={styles.modalContent}>
        <TextInput
          label={legacy ? "Database name" : "Branch name"}
          {...register("branchName", {
            required: "Database name is required",
            pattern: {
              value: /^[^@].*$/,
              message: legacy
                ? "Invalid database name"
                : "Invalid branch name",
            },
            validate: (v) =>
              v.startsWith("__") && v.endsWith("__")
                ? legacy
                  ? "Invalid database name"
                  : "Invalid branch name"
                : true,
          })}
          error={formState.errors.branchName?.message}
        />

        {!legacy ? (
          <>
            <Select
              label="From branch"
              items={[
                {id: null, label: <i>Empty</i>},
                ...(instanceState.databases ?? []).map(({name}) => ({
                  id: name,
                  label: name,
                })),
              ]}
              selectedItemId={watch("fromBranch")}
              onChange={({id}) => setValue("fromBranch", id)}
            />

            <Checkbox
              label="Copy data"
              checked={watch("copyData")}
              onChange={(checked) => setValue("copyData", checked)}
              disabled={watch("fromBranch") == null}
            />
          </>
        ) : null}
      </ModalContent>
    </Modal>
  );
}
