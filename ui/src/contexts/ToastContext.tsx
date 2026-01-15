import React, { createContext, useState, useCallback, ReactNode, useEffect } from 'react';
import { Toast, ToastData, ToastType } from '../components/Toast';
import { initializeToastService } from '../services/toast';

interface ToastContextValue {
  addToast: (type: ToastType, message: string, description?: string) => void;
  removeToast: (id: string) => void;
}

export const ToastContext = createContext<ToastContextValue | undefined>(undefined);

const MAX_TOASTS = 5;

const AUTO_DISMISS_DURATION = {
  success: 5000,
  error: 8000,
  warning: 8000,
  info: 5000,
};

interface ToastProviderProps {
  children: ReactNode;
}

export const ToastProvider: React.FC<ToastProviderProps> = ({ children }) => {
  const [toasts, setToasts] = useState<ToastData[]>([]);
  const [visibleToasts, setVisibleToasts] = useState<Set<string>>(new Set());

  const removeToast = useCallback((id: string) => {
    // First hide the toast with animation
    setVisibleToasts((prev) => {
      const next = new Set(prev);
      next.delete(id);
      return next;
    });

    // Then remove it from the array after animation completes
    setTimeout(() => {
      setToasts((prev) => prev.filter((toast) => toast.id !== id));
    }, 300); // Match the leave animation duration
  }, []);

  const addToast = useCallback(
    (type: ToastType, message: string, description?: string) => {
      const id = `toast-${Date.now()}-${Math.random()}`;
      const newToast: ToastData = {
        id,
        type,
        message,
        description,
      };

      setToasts((prev) => {
        const updated = [...prev, newToast];
        // Keep only the last MAX_TOASTS toasts
        return updated.slice(-MAX_TOASTS);
      });

      // Show the toast
      setVisibleToasts((prev) => new Set([...prev, id]));

      // Auto-dismiss after configured duration
      const duration = AUTO_DISMISS_DURATION[type];
      setTimeout(() => {
        removeToast(id);
      }, duration);
    },
    [removeToast]
  );

  // Initialize the imperative toast service for use outside React components
  useEffect(() => {
    initializeToastService(addToast);
  }, [addToast]);

  return (
    <ToastContext.Provider value={{ addToast, removeToast }}>
      {children}
      {/* Toast Container */}
      <div
        aria-live="polite"
        aria-atomic="true"
        className="fixed bottom-0 right-0 z-50 p-6 space-y-4 pointer-events-none"
      >
        <div className="space-y-4 pointer-events-auto">
          {toasts.map((toast) => (
            <Toast
              key={toast.id}
              toast={toast}
              show={visibleToasts.has(toast.id)}
              onDismiss={() => removeToast(toast.id)}
            />
          ))}
        </div>
      </div>
    </ToastContext.Provider>
  );
};
