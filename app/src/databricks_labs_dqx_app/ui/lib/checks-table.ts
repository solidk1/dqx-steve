type ChecksTableSettings = {
  default_checks_table?: string | null;
};

export const DEFAULT_CHECKS_TABLE_REQUIRED_MESSAGE =
  "Set a default rule table in Settings before loading rules.";

export function getRequiredChecksTableName(
  settings: ChecksTableSettings,
): string {
  const tableName = settings.default_checks_table?.trim();
  if (!tableName) {
    throw new Error(DEFAULT_CHECKS_TABLE_REQUIRED_MESSAGE);
  }
  return tableName;
}

export function getOptionalChecksTableName(
  settings: ChecksTableSettings | undefined,
): string | undefined {
  const tableName = settings?.default_checks_table?.trim();
  return tableName || undefined;
}
