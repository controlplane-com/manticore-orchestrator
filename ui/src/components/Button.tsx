import React from 'react';

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'primary' | 'secondary' | 'danger' | 'success';
  size?: 'xs' | 'sm' | 'md' | 'lg';
  children: React.ReactNode;
}

export const Button: React.FC<ButtonProps> = ({
  variant = 'primary',
  size = 'md',
  className = '',
  children,
  disabled,
  ...props
}) => {
  const baseClasses = 'inline-flex items-center justify-center font-medium rounded-md focus:outline-none focus:ring-2 focus:ring-offset-2 transition-colors';

  const variantClasses = {
    primary: 'bg-cpln-cyan text-white hover:bg-cpln-cyan-dark active:bg-cpln-cyan-dark active:scale-95 focus:ring-cpln-cyan disabled:bg-gray-300 dark:disabled:bg-gray-600 disabled:cursor-not-allowed',
    secondary: 'bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-200 border border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700 active:bg-gray-100 dark:active:bg-gray-600 active:scale-95 focus:ring-cpln-cyan disabled:bg-gray-100 dark:disabled:bg-gray-900 disabled:cursor-not-allowed',
    danger: 'bg-transparent text-orange-600 dark:text-orange-400 border-2 border-orange-500 hover:bg-orange-500/10 active:bg-orange-500/20 active:scale-95 focus:ring-orange-500 disabled:opacity-50 disabled:cursor-not-allowed',
    success: 'bg-green-600 text-white hover:bg-green-700 active:bg-green-800 active:scale-95 focus:ring-green-500 disabled:bg-green-300 disabled:cursor-not-allowed',
  };

  const sizeClasses = {
    xs: 'px-2 py-1 text-xs',
    sm: 'px-3 py-1.5 text-sm',
    md: 'px-4 py-2 text-sm',
    lg: 'px-6 py-3 text-base',
  };

  return (
    <button
      className={`${baseClasses} ${variantClasses[variant]} ${sizeClasses[size]} ${className}`}
      disabled={disabled}
      {...props}
    >
      {children}
    </button>
  );
};
