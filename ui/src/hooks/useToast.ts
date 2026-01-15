import { useContext } from 'react';
import { ToastContext } from '../contexts/ToastContext';

export const useToast = () => {
  const context = useContext(ToastContext);

  if (!context) {
    throw new Error('useToast must be used within a ToastProvider');
  }

  const { addToast } = context;

  return {
    success: (message: string, description?: string) => {
      addToast('success', message, description);
    },
    error: (message: string, description?: string) => {
      addToast('error', message, description);
    },
    warning: (message: string, description?: string) => {
      addToast('warning', message, description);
    },
    info: (message: string, description?: string) => {
      addToast('info', message, description);
    },
  };
};
