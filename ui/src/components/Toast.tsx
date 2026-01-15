import { Fragment } from 'react';
import { Transition } from '@headlessui/react';
import { XMarkIcon, CheckCircleIcon, ExclamationCircleIcon, InformationCircleIcon, ExclamationTriangleIcon } from '@heroicons/react/24/outline';

export type ToastType = 'success' | 'error' | 'warning' | 'info';

export interface ToastData {
  id: string;
  type: ToastType;
  message: string;
  description?: string;
}

interface ToastProps {
  toast: ToastData;
  show: boolean;
  onDismiss: () => void;
}

const toastStyles = {
  success: {
    container: 'bg-green-50 dark:bg-green-900 border-green-500',
    icon: 'text-green-500',
    text: 'text-green-900 dark:text-green-100',
    Icon: CheckCircleIcon,
  },
  error: {
    container: 'bg-red-50 dark:bg-red-900 border-red-500',
    icon: 'text-red-500',
    text: 'text-red-900 dark:text-red-100',
    Icon: ExclamationCircleIcon,
  },
  warning: {
    container: 'bg-yellow-50 dark:bg-yellow-900 border-yellow-500',
    icon: 'text-yellow-500',
    text: 'text-yellow-900 dark:text-yellow-100',
    Icon: ExclamationTriangleIcon,
  },
  info: {
    container: 'bg-blue-50 dark:bg-blue-900 border-blue-500',
    icon: 'text-blue-500',
    text: 'text-blue-900 dark:text-blue-100',
    Icon: InformationCircleIcon,
  },
};

export const Toast: React.FC<ToastProps> = ({ toast, show, onDismiss }) => {
  const style = toastStyles[toast.type];
  const Icon = style.Icon;

  return (
    <Transition
      show={show}
      as={Fragment}
      enter="transform ease-out duration-300 transition"
      enterFrom="translate-x-full opacity-0"
      enterTo="translate-x-0 opacity-100"
      leave="transform ease-in duration-200 transition"
      leaveFrom="translate-x-0 opacity-100"
      leaveTo="translate-x-full opacity-0"
    >
      <div
        className={`max-w-sm w-full border-l-4 rounded-lg shadow-lg p-4 ${style.container}`}
        role="alert"
      >
        <div className="flex items-start">
          <div className="flex-shrink-0">
            <Icon className={`h-6 w-6 ${style.icon}`} aria-hidden="true" />
          </div>
          <div className="ml-3 flex-1">
            <p className={`text-sm font-medium ${style.text}`}>
              {toast.message}
            </p>
            {toast.description && (
              <p className={`mt-1 text-sm ${style.text} opacity-90`}>
                {toast.description}
              </p>
            )}
          </div>
          <div className="ml-4 flex-shrink-0">
            <button
              type="button"
              className={`inline-flex rounded-md ${style.text} opacity-70 hover:opacity-100 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-${toast.type}-500`}
              onClick={onDismiss}
            >
              <span className="sr-only">Close</span>
              <XMarkIcon className="h-5 w-5" aria-hidden="true" />
            </button>
          </div>
        </div>
      </div>
    </Transition>
  );
};
