import {useEffect, useState} from "react";
import {useForm} from "react-hook-form";

import {
  ModalPanel,
  ArrowRightIcon,
  ModalContent,
  TextInput,
  SubmitButton,
} from "@dbsof/common/newui";

import {auth} from "@dbsof/auth";
import {getHTTPSCRAMAuth} from "@dbsof/platform/client";
import {Logo} from "../header";

import styles from "./loginPage.module.scss";

// Initialize auth with login function
const httpSCRAMAuth = getHTTPSCRAMAuth();
auth.setLoginFunction(httpSCRAMAuth);

export default function LoginPage() {
  const [error, setError] = useState<string | null>(null);
  const {register, handleSubmit, formState, setFocus} = useForm<{
    username: string;
    password: string;
  }>({mode: "onChange"});

  useEffect(
    () => {
      setFocus("username");
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    []
  );

  const onSubmit = handleSubmit(async ({username, password}) => {
    setError(null);
    try {
      await auth.login(username, password);
    } catch (err) {
      console.error(err);
      setError("Login failed: username or password may be incorrect");
    }
  });

  return (
    <div className={styles.loginPage}>
      <Logo className={styles.logo} />

      <ModalPanel
        className={styles.loginPanel}
        title="Welcome to Dbsof"
        onSubmit={onSubmit}
        formError={error}
        footerButtons={
          <SubmitButton
            kind="primary"
            loading={formState.isSubmitting}
            disabled={!formState.isValid || formState.isSubmitting}
            rightIcon={<ArrowRightIcon />}
          >
            Login
          </SubmitButton>
        }
      >
        <ModalContent className={styles.formContent}>
          <TextInput
            label="Username"
            {...register("username", {
              required: "Username is required",
            })}
            error={formState.errors.username?.message}
          />
          <TextInput
            label="Password"
            type="password"
            {...register("password", {
              required: "Password is required",
            })}
            error={formState.errors.password?.message}
          />
        </ModalContent>
      </ModalPanel>
    </div>
  );
}
