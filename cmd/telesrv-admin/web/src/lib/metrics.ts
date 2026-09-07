import type { AccountRow, ChannelRow } from "../types";

export function accountMetrics(rows: AccountRow[]) {
  return rows.reduce(
    (acc, row) => {
      acc.devices += row.DeviceCount;
      if (row.PremiumUntil > 0) acc.premium += 1;
      if (row.Frozen) acc.frozen += 1;
      return acc;
    },
    { devices: 0, premium: 0, frozen: 0 }
  );
}

export function channelMetrics(rows: ChannelRow[]) {
  return rows.reduce(
    (acc, row) => {
      if (row.Megagroup) acc.megagroups += 1;
      if (row.Broadcast) acc.broadcasts += 1;
      if (row.Verified) acc.verified += 1;
      return acc;
    },
    { megagroups: 0, broadcasts: 0, verified: 0 }
  );
}
