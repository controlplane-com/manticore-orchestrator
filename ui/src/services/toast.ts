import { ToastType } from '../components/Toast';

// This module provides an imperative toast API that can be used outside React components
// It's initialized by the ToastProvider and can be called from anywhere (e.g., axios interceptors)

type ToastFunction = (type: ToastType, message: string, description?: string) => void;

let globalAddToast: ToastFunction | null = null;

export const initializeToastService = (addToast: ToastFunction) => {
  globalAddToast = addToast;
};

export const toast = {
  success: (message: string, description?: string) => {
    if (globalAddToast) {
      globalAddToast('success', message, description);
    }
  },
  error: (message: string, description?: string) => {
    if (globalAddToast) {
      globalAddToast('error', message, description);
    }
  },
  warning: (message: string, description?: string) => {
    if (globalAddToast) {
      globalAddToast('warning', message, description);
    }
  },
  info: (message: string, description?: string) => {
    if (globalAddToast) {
      globalAddToast('info', message, description);
    }
  },
};
