import { Cable, LogOut, ShieldCheck } from "lucide-react";
import { formatDate } from "../lib/format";
import { useI18n } from "../i18n";
import type { AuthorizationRow } from "../types";
import { ActionButton } from "./ActionButton";
import { EmptyRow } from "./ui";

export function AuthorizationTable({ rows, userID, onDone }: { rows: AuthorizationRow[]; userID: number; onDone: () => void }) {
  const { t } = useI18n();

  return (
    <div className="authorization-block">
      <div className="table-wrap">
        <table className="data-table authorization-table">
          <thead>
            <tr>
              <th>{t("auth.device")}</th>
              <th>{t("auth.platform")}</th>
              <th>{t("auth.ip")}</th>
              <th>{t("auth.lastActive")}</th>
              <th className="device-actions-head">{t("common.actions")}</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.Hash}>
                <td className="device-text">{row.DeviceModel} {row.SystemVersion}</td>
                <td className="device-text">{row.Platform} {row.AppVersion}</td>
                <td>{row.IP}</td>
                <td>{formatDate(row.ActiveAt)}</td>
                <td className="device-actions-cell">
                  <div className="device-actions">
                    <ActionButton
                      label={t("auth.revokeCurrent")}
                      icon={<LogOut size={13} />}
                      compact
                      path="/api/actions/revoke-sessions"
                      payload={() => ({ user_id: userID, hash: row.Hash })}
                      onDone={onDone}
                    />
                    <ActionButton
                      label={t("auth.keepCurrent")}
                      icon={<ShieldCheck size={13} />}
                      compact
                      path="/api/actions/revoke-sessions"
                      payload={() => ({ user_id: userID, keep_hash: row.Hash })}
                      onDone={onDone}
                    />
                  </div>
                </td>
              </tr>
            ))}
            {rows.length === 0 && <EmptyRow colSpan={5} />}
          </tbody>
        </table>
      </div>
      <div className="danger-zone">
        <ActionButton
          label={t("auth.revokeAll")}
          icon={<Cable size={15} />}
          path="/api/actions/revoke-sessions"
          payload={() => ({ user_id: userID, revoke_all: true })}
          onDone={onDone}
        />
      </div>
    </div>
  );
}
