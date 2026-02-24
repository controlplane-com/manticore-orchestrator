const LOCK_KEY = 'manticore-locked-tables';

export const getLockedTables = (): string[] => {
  try {
    return JSON.parse(localStorage.getItem(LOCK_KEY) ?? '[]');
  } catch {
    return [];
  }
};

export const isTableLocked = (name: string): boolean =>
  getLockedTables().includes(name);

export const lockTable = (name: string): string[] => {
  const current = getLockedTables();
  const next = current.includes(name) ? current : [...current, name];
  localStorage.setItem(LOCK_KEY, JSON.stringify(next));
  return next;
};

export const unlockTable = (name: string): string[] => {
  const next = getLockedTables().filter(t => t !== name);
  localStorage.setItem(LOCK_KEY, JSON.stringify(next));
  return next;
};
