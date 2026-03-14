import {observer} from "mobx-react-lite";
import {Link} from "react-router-dom";

import {HeaderTabs} from "@dbsof/studio/components/headerNav";

import {ThemeSwitcher} from "@dbsof/common/ui/themeSwitcher";
import {UserMenu} from "@dbsof/common/newui/userMenu";
import {UserIcon} from "@dbsof/common/newui";
import cn from "@dbsof/common/utils/classNames";
import {TabSettingsIcon} from "@dbsof/studio/icons";

import {LogoLocal} from "@dbsof/common/ui/icons/logo";

import appState from "../../state/store";
import {auth} from "@dbsof/auth";

import styles from "./header.module.scss";

export const Logo = ({className}: {className?: string}) => {
  return (
    <Link to="/" className={cn(styles.logoLink, className)}>
      <LogoLocal className={styles.logo} />
    </Link>
  );
};

export const Header = observer(function Header() {
  const username = auth.getCurrentUsername();

  return (
    <div className={styles.header}>
      <Logo />
      <HeaderTabs keys={["instance", "database"]} />

      <div className={styles.controls}>
        <Link to="/settings" className={styles.settingsLink}>
          <TabSettingsIcon active={false} />
        </Link>
        <ThemeSwitcher className={styles.themeSwitcher} />
        <UserMenu
          className={styles.userMenu}
          avatar={
            username ? (
              <div className={styles.userInitial}>
                {username.slice(0, 2).toUpperCase()}
              </div>
            ) : (
              <UserIcon className={styles.userIcon} />
            )
          }
          name={username ?? "admin"}
          signout={{action: () => auth.logout()}}
        />
      </div>
    </div>
  );
});
